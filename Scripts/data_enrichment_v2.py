import pandas as pd
import json
import time
from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.tmdb_fetcher import TMDbFetcher
from utils.logger import log_info, log_error
from datetime import datetime
import os

class ModifiedDataEnrichment:
    def __init__(self):
        self.tmdb = TMDbFetcher()
        self.enrichment_stats = {
            'processed': 0,
            'enriched': 0,
            'failed': 0,
            'total_api_calls': 0
        }
        # Define the specific columns we want to enrich
        self.target_columns = [
            'budget',
            'revenue', 
            'genres',
            'production_companies',
            'production_countries',
            'spoken_languages'
        ]
    
    def find_movie_by_search(self, title, release_year=None):
        """
        Search for movie using title and optionally release year
        Returns the best match movie ID or None
        """
        try:
            # Clean title for search
            search_title = str(title).strip() if title else ""
            if not search_title or search_title.lower() == 'nan':
                return None
            
            log_info(f"Searching for movie: '{search_title}' ({release_year})")
            
            # Search with year if available
            search_results = self.tmdb.search_movie(search_title, year=release_year)
            
            if not search_results or 'results' not in search_results:
                # Try without year if first search failed
                if release_year:
                    log_info(f"Retrying search without year for: '{search_title}'")
                    search_results = self.tmdb.search_movie(search_title)
            
            if search_results and 'results' in search_results and search_results['results']:
                # Get the first (most relevant) result
                best_match = search_results['results'][0]
                movie_id = best_match.get('id')
                found_title = best_match.get('title', 'Unknown')
                found_year = best_match.get('release_date', '')[:4] if best_match.get('release_date') else 'Unknown'
                
                log_info(f"Found match: ID={movie_id}, Title='{found_title}', Year={found_year}")
                return movie_id
            
            log_error(f"No search results found for: '{search_title}'")
            return None
            
        except Exception as e:
            log_error(f"Search failed for '{title}': {e}")
            return None
    
    def extract_release_year(self, release_date):
        """Extract year from release_date string"""
        if not release_date or str(release_date).strip() == '' or str(release_date) == 'nan':
            return None
            
        try:
            # Handle different date formats
            date_str = str(release_date).strip()
            
            # If it's in DD/MM/YYYY format (as in your data)
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    year = parts[2]  # Last part should be year
                    if len(year) == 4 and year.isdigit():
                        return int(year)
            
            # If it's in YYYY-MM-DD format (TMDb format)
            elif '-' in date_str:
                year = date_str[:4]
                if len(year) == 4 and year.isdigit():
                    return int(year)
            
            # If it's just a year
            elif len(date_str) == 4 and date_str.isdigit():
                return int(date_str)
                
        except Exception as e:
            log_error(f"Error extracting year from '{release_date}': {e}")
            
        return None
    
    def is_field_missing(self, value, field_name):
        """
        Check if a field is missing or needs enrichment
        """
        if pd.isna(value) or value == "" or str(value).strip() == "" or str(value).lower() == 'nan':
            return True
            
        # For numeric fields (budget, revenue), treat 0 as missing
        if field_name in ['budget', 'revenue']:
            try:
                numeric_value = float(str(value))
                return numeric_value == 0
            except (ValueError, TypeError):
                return True
                
        # For list-type fields, check for empty brackets or empty strings
        if field_name in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
            str_value = str(value).strip()
            return str_value in ['', '[]', 'nan']
            
        return False
    
    def enrich_movie_row(self, row):
        """
        Enrich a single movie row with missing data from the target columns
        Returns: (enriched_row, was_enriched)
        """
        movie_id = row.get('id')
        title = row.get('title')
        release_date = row.get('release_date')
        
        # Extract year from release_date if available
        release_year = self.extract_release_year(release_date)
        
        # Check what target data is missing
        missing_fields = []
        for field in self.target_columns:
            value = row.get(field)
            if self.is_field_missing(value, field):
                missing_fields.append(field)
        
        if not missing_fields:
            log_info(f"Movie ID {movie_id}: No missing target data")
            return row, False
        
        log_info(f"Movie ID {movie_id} ('{title}'): Missing fields: {missing_fields}")
        
        # Try to fetch data using movie ID first
        tmdb_data = None
        if movie_id and str(movie_id) != 'nan' and str(movie_id).strip() != '':
            try:
                tmdb_data = self.tmdb.fetch_movie_details(movie_id)
                self.enrichment_stats['total_api_calls'] += 1
            except Exception as e:
                log_error(f"Failed to fetch with ID {movie_id}: {e}")
        
        # If ID fetch failed and we have title, try search
        if not tmdb_data and title and str(title).strip() != '':
            try:
                search_movie_id = self.find_movie_by_search(title, release_year)
                if search_movie_id:
                    tmdb_data = self.tmdb.fetch_movie_details(search_movie_id)
                    self.enrichment_stats['total_api_calls'] += 1
                    # Update the ID with the found one if it was missing
                    if self.is_field_missing(row.get('id'), 'id'):
                        row['id'] = search_movie_id
            except Exception as e:
                log_error(f"Search and fetch failed for '{title}': {e}")
        
        # Fill missing data from TMDb response
        if tmdb_data:
            enriched = False
            
            # Map TMDb fields to our columns with proper processing
            for field in missing_fields:
                if field in ['budget', 'revenue']:
                    # Direct numeric mapping
                    if field in tmdb_data and tmdb_data[field] is not None:
                        tmdb_value = tmdb_data[field]
                        if tmdb_value and tmdb_value != 0:  # Only use non-zero values
                            row[field] = tmdb_value
                            enriched = True
                            log_info(f"Filled {field}: {tmdb_value}")
                
                elif field in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
                    # List fields - convert to comma-separated strings
                    if field in tmdb_data and tmdb_data[field]:
                        tmdb_value = tmdb_data[field]
                        if isinstance(tmdb_value, list) and tmdb_value:
                            # Convert list to comma-separated string
                            row[field] = ', '.join(str(item) for item in tmdb_value)
                            enriched = True
                            log_info(f"Filled {field}: {row[field]}")
                        elif isinstance(tmdb_value, str) and tmdb_value.strip():
                            row[field] = tmdb_value
                            enriched = True
                            log_info(f"Filled {field}: {tmdb_value}")
            
            return row, enriched
        else:
            log_error(f"Could not enrich movie ID {movie_id} / '{title}'")
            return row, False
    
    def enrich_dataset(self, df):
        """Enrich the movie dataset with target columns only"""
        log_info("Starting enrichment of movie dataset")
        log_info(f"Total rows to process: {len(df)}")
        log_info(f"Target columns: {', '.join(self.target_columns)}")
        
        enriched_rows = []
        
        for index, row in df.iterrows():
            try:
                self.enrichment_stats['processed'] += 1
                enriched_row, was_enriched = self.enrich_movie_row(row.to_dict())
                
                if was_enriched:
                    self.enrichment_stats['enriched'] += 1
                    log_info(f"Successfully enriched row {index + 1}")
                
                enriched_rows.append(enriched_row)
                
                # Progress logging
                if (index + 1) % 25 == 0:
                    log_info(f"Progress: {index + 1}/{len(df)} rows processed")
                
            except Exception as e:
                log_error(f"Error processing row {index + 1}: {e}")
                self.enrichment_stats['failed'] += 1
                enriched_rows.append(row.to_dict())
        
        return pd.DataFrame(enriched_rows)
    
    def print_enrichment_summary(self):
        """Print a summary of the enrichment process"""
        print("\n" + "="*70)
        print("DATA ENRICHMENT SUMMARY")
        print("="*70)
        print(f"Target columns: {', '.join(self.target_columns)}")
        print(f"\nProcessed: {self.enrichment_stats['processed']} rows")
        print(f"Enriched:  {self.enrichment_stats['enriched']} rows")
        print(f"Failed:    {self.enrichment_stats['failed']} rows")
        
        if self.enrichment_stats['processed'] > 0:
            success_rate = (self.enrichment_stats['enriched'] / self.enrichment_stats['processed']) * 100
            print(f"Success:   {success_rate:.1f}%")
        
        print(f"\nTotal API calls made: {self.enrichment_stats['total_api_calls']}")
        print("="*70)

def main():
    """Main enrichment process for the specific dataset"""
    try:
        enricher = ModifiedDataEnrichment()
        
        log_info("Starting targeted data enrichment process...")
        start_time = datetime.now()

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        ROOT_DIR = os.path.dirname(BASE_DIR)
        MOVIES_MAIN_PATH = os.path.join(ROOT_DIR,"Scripts","TMDB_all_movies.csv") 
        output_file_path = "movies_dataset_enriched.csv"
        
        # Load the dataset
        log_info(f"Loading dataset from {MOVIES_MAIN_PATH}...")
        df = read_csv(MOVIES_MAIN_PATH)
        
        log_info(f"Loaded {len(df)} rows from dataset")
        log_info(f"Dataset columns: {list(df.columns)}")
        
        # Check which target columns exist in the dataset
        existing_target_cols = [col for col in enricher.target_columns if col in df.columns]
        missing_target_cols = [col for col in enricher.target_columns if col not in df.columns]
        
        if missing_target_cols:
            log_info(f"Note: These target columns are missing from dataset and will be added: {missing_target_cols}")
            # Add missing columns with empty values
            for col in missing_target_cols:
                df[col] = ""
        
        log_info(f"Will attempt to enrich these columns: {existing_target_cols}")
        
        # Enrich the dataset
        enriched_df = enricher.enrich_dataset(df)
        
        # Save enriched file
        log_info(f"Saving enriched dataset to {output_file_path}...")
        write_csv(enriched_df, output_file_path)
        
        # Calculate total time
        end_time = datetime.now()
        total_time = end_time - start_time
        
        # Print summary
        enricher.print_enrichment_summary()
        print(f"\nTotal processing time: {total_time}")
        print(f"Enriched file saved: {output_file_path}")
        
        log_info("Targeted data enrichment completed successfully!")
        
    except Exception as e:
        log_error(f"Critical error in enrichment process: {e}")
        raise

if __name__ == "__main__":
    main()