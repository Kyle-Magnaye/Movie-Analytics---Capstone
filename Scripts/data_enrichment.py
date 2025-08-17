import pandas as pd
import json
import time
from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.tmdb_fetcher import TMDbFetcher
from utils.logger import log_info, log_error
from datetime import datetime
import os

class DataEnrichment:
    def __init__(self):
        self.tmdb = TMDbFetcher()
        self.enrichment_stats = {
            'movies_main': {'processed': 0, 'enriched': 0, 'failed': 0},
            'movie_extended': {'processed': 0, 'enriched': 0, 'failed': 0},
            'total_api_calls': 0
        }
    
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
    
    def enrich_movies_main_row(self, row):
        """
        Enrich a single row from movies_main.csv with all missing data at once
        Returns: (enriched_row, was_enriched)
        """
        movie_id = row.get('id')
        title = row.get('title')
        release_date = row.get('release_date')
        
        # Extract year from release_date if available
        release_year = None
        if release_date and str(release_date).strip() and str(release_date) != 'nan':
            try:
                release_year = str(release_date)[:4]
                if len(release_year) == 4 and release_year.isdigit():
                    release_year = int(release_year)
                else:
                    release_year = None
            except:
                release_year = None
        
        # Check what data is missing
        missing_fields = []
        for field in ['title', 'release_date', 'budget', 'revenue']:
            value = row.get(field)
            
            # Different logic for different field types
            if field in ['title', 'release_date']:
                # For text fields, check for null/empty/nan
                if (pd.isna(value) or value == "" or 
                    str(value).strip() == "" or str(value).lower() == 'nan'):
                    missing_fields.append(field)
            elif field in ['budget', 'revenue']:
                # For numeric fields, treat 0 as missing data (since 0 budget/revenue usually means unknown)
                if (pd.isna(value) or value == "" or value == 0 or value == "0" or
                    str(value).strip() == "" or str(value).lower() == 'nan'):
                    missing_fields.append(field)
        
        if not missing_fields:
            log_info(f"Movie ID {movie_id}: No missing data")
            return row, False
        
        log_info(f"Movie ID {movie_id}: Missing fields: {missing_fields}")
        
        # Try to fetch data using movie ID first
        tmdb_data = None
        if movie_id and str(movie_id) != 'nan':
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
                    # Update the ID with the found one
                    row['id'] = search_movie_id
            except Exception as e:
                log_error(f"Search and fetch failed for '{title}': {e}")
        
        # Fill missing data from TMDb response
        if tmdb_data:
            enriched = False
            
            # Map TMDb fields to our columns
            field_mapping = {
                'title': 'title',
                'release_date': 'release_date', 
                'budget': 'budget',
                'revenue': 'revenue'
            }
            
            for our_field, tmdb_field in field_mapping.items():
                if our_field in missing_fields and tmdb_field in tmdb_data:
                    tmdb_value = tmdb_data[tmdb_field]
                    if tmdb_value is not None and str(tmdb_value) != "":
                        row[our_field] = tmdb_value
                        enriched = True
                        log_info(f"Filled {our_field}: {tmdb_value}")
            
            return row, enriched
        else:
            log_error(f"Could not enrich movie ID {movie_id} / '{title}'")
            return row, False
    
    def enrich_movie_extended_row(self, row):
        """
        Enrich a single row from movie_extended.csv with all missing data at once
        """
        movie_id = row.get('id')
        
        # Check what data is missing
        missing_fields = []
        for field in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
            value = row.get(field)
            if (pd.isna(value) or value == "" or value == "[]" or
                str(value).strip() == "" or str(value).lower() == 'nan'):
                missing_fields.append(field)
        
        if not missing_fields:
            return row, False
        
        log_info(f"Movie ID {movie_id}: Missing extended fields: {missing_fields}")
        
        # Fetch data from TMDb
        try:
            tmdb_data = self.tmdb.fetch_movie_details(movie_id)
            self.enrichment_stats['total_api_calls'] += 1
            
            if tmdb_data:
                enriched = False
                
                # Fill missing fields
                for field in missing_fields:
                    if field in tmdb_data and tmdb_data[field]:
                        # Convert lists to comma-separated strings for CSV storage
                        if isinstance(tmdb_data[field], list):
                            row[field] = ', '.join(tmdb_data[field])
                        else:
                            row[field] = tmdb_data[field]
                        enriched = True
                        log_info(f"Filled {field}: {row[field]}")
                
                return row, enriched
            
        except Exception as e:
            log_error(f"Failed to enrich extended data for movie ID {movie_id}: {e}")
        
        return row, False
    
    def enrich_movies_main(self, df):
        """Enrich the movies_main.csv DataFrame"""
        log_info("Starting enrichment of movies_main.csv")
        log_info(f"Total rows to process: {len(df)}")
        
        enriched_rows = []
        
        for index, row in df.iterrows():
            try:
                self.enrichment_stats['movies_main']['processed'] += 1
                enriched_row, was_enriched = self.enrich_movies_main_row(row.to_dict())
                
                if was_enriched:
                    self.enrichment_stats['movies_main']['enriched'] += 1
                    log_info(f"Successfully enriched row {index + 1}")
                
                enriched_rows.append(enriched_row)
                
                # Progress logging
                if (index + 1) % 50 == 0:
                    log_info(f"Progress: {index + 1}/{len(df)} rows processed")
                
            except Exception as e:
                log_error(f"Error processing row {index + 1}: {e}")
                self.enrichment_stats['movies_main']['failed'] += 1
                enriched_rows.append(row.to_dict())
        
        return pd.DataFrame(enriched_rows)
    
    def enrich_movie_extended(self, df):
        """Enrich the movie_extended.csv DataFrame"""
        log_info("Starting enrichment of movie_extended.csv")
        log_info(f"Total rows to process: {len(df)}")
        
        enriched_rows = []
        
        for index, row in df.iterrows():
            try:
                self.enrichment_stats['movie_extended']['processed'] += 1
                enriched_row, was_enriched = self.enrich_movie_extended_row(row.to_dict())
                
                if was_enriched:
                    self.enrichment_stats['movie_extended']['enriched'] += 1
                
                enriched_rows.append(enriched_row)
                
                # Progress logging
                if (index + 1) % 50 == 0:
                    log_info(f"Progress: {index + 1}/{len(df)} rows processed")
                
                # Small delay to be respectful to the API
                time.sleep(0.1)
                
            except Exception as e:
                log_error(f"Error processing extended row {index + 1}: {e}")
                self.enrichment_stats['movie_extended']['failed'] += 1
                enriched_rows.append(row.to_dict())
        
        return pd.DataFrame(enriched_rows)
    
    def print_enrichment_summary(self):
        """Print a summary of the enrichment process"""
        print("\n" + "="*60)
        print("DATA ENRICHMENT SUMMARY")
        print("="*60)
        
        for dataset, stats in self.enrichment_stats.items():
            if dataset != 'total_api_calls':
                print(f"\n{dataset.upper()}:")
                print(f"  Processed: {stats['processed']} rows")
                print(f"  Enriched:  {stats['enriched']} rows")
                print(f"  Failed:    {stats['failed']} rows")
                if stats['processed'] > 0:
                    success_rate = ((stats['enriched']) / stats['processed']) * 100
                    print(f"  Success:   {success_rate:.1f}%")
        
        print(f"\nTotal API calls made: {self.enrichment_stats['total_api_calls']}")
        print("="*60)

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    MOVIES_MAIN_PATH = os.path.join("movies_main_enriched.csv")
    MOVIE_EXTENDED_PATH = os.path.join("movie_extended_enriched.csv")
    """Main enrichment process"""
    try:
        enricher = DataEnrichment()
        
        log_info("Starting data enrichment process...")
        start_time = datetime.now()
        
        # Load original data files
        log_info("Loading original data files...")
        movies_main = read_csv(MOVIES_MAIN_PATH)
        movie_extended = read_csv(MOVIE_EXTENDED_PATH)
        
        log_info(f"Loaded {len(movies_main)} rows from movies_main.csv")
        log_info(f"Loaded {len(movie_extended)} rows from movie_extended.csv")
        
        # Enrich datasets
        movies_main_enriched = enricher.enrich_movies_main(movies_main)
        movie_extended_enriched = enricher.enrich_movie_extended(movie_extended)
        
        # Save enriched files
        log_info("Saving enriched files...")
        write_csv(movies_main_enriched, "movies_main_enriched.csv")
        write_csv(movie_extended_enriched, "movie_extended_enriched.csv")
        
        # Calculate total time
        end_time = datetime.now()
        total_time = end_time - start_time
        
        # Print summary
        enricher.print_enrichment_summary()
        print(f"\nTotal processing time: {total_time}")
        print(f"Enriched files saved:")
        print(f"  - movies_main_enriched.csv")
        print(f"  - movie_extended_enriched.csv")
        print(f"\nNext step: Run main.py for data cleaning on enriched files")
        
        log_info("Data enrichment completed successfully!")
        
    except Exception as e:
        log_error(f"Critical error in enrichment process: {e}")
        raise

if __name__ == "__main__":
    main()