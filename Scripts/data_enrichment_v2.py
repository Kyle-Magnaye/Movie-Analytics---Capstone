import pandas as pd
import json
import time
from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.tmdb_fetcher import TMDbFetcher
from utils.logger import log_info, log_error
from datetime import datetime
import os
import pickle

class ModifiedDataEnrichment:
    def __init__(self, checkpoint_interval=1000):
        self.tmdb = TMDbFetcher()
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = "enrichment_checkpoint.pkl"
        self.progress_file = "enrichment_progress.json"
        
        self.enrichment_stats = {
            'processed': 0,
            'enriched': 0,
            'failed': 0,
            'total_api_calls': 0,
            'start_time': None,
            'last_checkpoint': 0
        }
        
        # Define the specific columns we want to enrich (excluding 'id')
        self.target_columns = [
            'title',
            'release_date',
            'budget',
            'revenue', 
            'vote_average',
            'vote_count',
            'popularity',
            'rating',
            'runtime',
            'genres',
            'keywords',
            'production_companies',
            'production_countries',
            'spoken_languages',
            'director',
            'writers',  # Screenplay writers
            'cast_top_5'  # Top 5 cast members
        ]
        
        # Define the final columns we want in the output CSV
        self.output_columns = [
            'id',
            'title',
            'release_date',
            'vote_average',
            'vote_count',
            'budget',
            'revenue',
            'popularity',
            'rating',
            'runtime',
            'genres',
            'keywords',
            'production_companies',
            'production_countries',
            'spoken_languages',
            'director',
            'writers',
            'cast_top_5'
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
    
    def save_checkpoint(self, enriched_rows, current_index):
        """Save current progress to checkpoint file"""
        checkpoint_data = {
            'enriched_rows': enriched_rows,
            'current_index': current_index,
            'stats': self.enrichment_stats,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            
            # Also save a JSON progress file for easy reading
            progress_data = {
                'current_index': current_index,
                'total_processed': self.enrichment_stats['processed'],
                'total_enriched': self.enrichment_stats['enriched'],
                'total_failed': self.enrichment_stats['failed'],
                'api_calls': self.enrichment_stats['total_api_calls'],
                'timestamp': datetime.now().isoformat(),
                'completion_percentage': 0  # Will be set by caller
            }
            
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
                
            log_info(f"Checkpoint saved at row {current_index}")
            
        except Exception as e:
            log_error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self):
        """Load progress from checkpoint file"""
        if not os.path.exists(self.checkpoint_file):
            log_info("No checkpoint file found, starting fresh")
            return None, 0
        
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint_data = pickle.load(f)
            
            self.enrichment_stats = checkpoint_data['stats']
            log_info(f"Checkpoint loaded from row {checkpoint_data['current_index']}")
            log_info(f"Previous stats: {checkpoint_data['stats']['processed']} processed, "
                    f"{checkpoint_data['stats']['enriched']} enriched, "
                    f"{checkpoint_data['stats']['api_calls']} API calls")
            
            return checkpoint_data['enriched_rows'], checkpoint_data['current_index']
            
        except Exception as e:
            log_error(f"Failed to load checkpoint: {e}")
            return None, 0
    
    def cleanup_checkpoint_files(self):
        """Remove checkpoint files after successful completion"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
            log_info("Checkpoint files cleaned up")
        except Exception as e:
            log_error(f"Failed to cleanup checkpoint files: {e}")
    
    def is_field_missing(self, value, field_name):
        """
        Check if a field is missing or needs enrichment
        """
        # Handle basic missing values
        if pd.isna(value) or value is None:
            return True
            
        # Convert to string for consistent checking
        str_value = str(value).strip()
        
        # Check for common missing value indicators
        if str_value in ['', 'nan', 'NaN', 'NULL', 'null', 'None','[]','[ ]']:
            return True
            
        # For numeric fields (budget, revenue, vote_count, popularity, runtime), treat 0 as missing
        if field_name in ['budget', 'revenue', 'vote_count', 'popularity', 'runtime']:
            try:
                numeric_value = float(str_value)
                return numeric_value == 0 or numeric_value < 0  
            except (ValueError, TypeError):
                return True
                
        # For rating fields (vote_average, rating), treat 0 as missing
        if field_name in ['vote_average', 'rating']:
            try:
                rating_value = float(str_value)
                return rating_value == 0 or rating_value < 0
            except (ValueError, TypeError):
                return True
        
        # For list-type fields, check for empty brackets and variations
        if field_name in ['genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages']:
            # Remove all whitespace and check for empty list indicators
            cleaned_value = str_value.replace(' ', '').replace('\t', '').replace('\n', '')
            empty_indicators = ['[]', '[ ]', '[,]', '""', "''", '{}', 'false', 'FALSE']
            return cleaned_value.lower() in [ind.lower() for ind in empty_indicators]
        
        # For text fields, check for meaningful content
        if field_name in ['title', 'director', 'writers', 'cast_top_5',]:
            # These should have actual text content
            meaningful_content = str_value not in ['', '0', 'false', 'FALSE', 'unknown', 'Unknown', 'N/A', 'n/a']
            return not meaningful_content
        
        # For boolean fields
        if field_name in ['adult']:
            return str_value.lower() in ['', 'nan', 'none', 'null']
            
        return False
    
    def get_writers_from_credits(self, credits_data):
        """
        Extract writers/screenplay writers from credits
        Returns comma-separated string of writer names
        """
        try:
            if not credits_data or 'crew' not in credits_data:
                return None
            
            writers = []
            writer_jobs = ['Writer', 'Screenplay', 'Story', 'Novel', 'Adaptation']
            
            for crew_member in credits_data['crew']:
                job = crew_member.get('job', '')
                if any(writer_job in job for writer_job in writer_jobs):
                    name = crew_member.get('name')
                    if name and name not in writers:
                        writers.append(name)
            
            return ', '.join(writers[:5]) if writers else None  # Limit to top 5 writers
            
        except Exception as e:
            log_error(f"Error extracting writers: {e}")
            return None
    
    def get_top_cast_from_credits(self, credits_data):
        """
        Extract top 5 cast members from credits
        Returns comma-separated string of actor names
        """
        try:
            if not credits_data or 'cast' not in credits_data:
                return None
            
            # Get top 5 cast members by order
            cast_list = credits_data['cast'][:5]
            cast_names = [actor.get('name') for actor in cast_list if actor.get('name')]
            
            return ', '.join(cast_names) if cast_names else None
            
        except Exception as e:
            log_error(f"Error extracting cast: {e}")
            return None
    
    def enrich_movie_row(self, row):
        """
        Enrich a single movie row with missing data from ALL target columns (except id)
        Returns: (enriched_row, was_enriched)
        """
        movie_id = row.get('id')
        title = row.get('title')
        release_date = row.get('release_date')
        
        # Extract year from release_date if available
        release_year = self.extract_release_year(release_date)
        
        # Check ALL target columns for missing data (except id)
        missing_fields = []
        for field in self.target_columns:
            if field != 'id':  # Skip id column
                value = row.get(field)
                if self.is_field_missing(value, field):
                    missing_fields.append(field)
        
        if not missing_fields:
            log_info(f"Movie ID {movie_id}: No missing data")
            return row, False
        
        log_info(f"Movie ID {movie_id} ('{title}'): Missing fields: {missing_fields}")
        
        # Try to fetch data using movie ID first
        tmdb_data = None
        credits_data = None
        
        if movie_id and str(movie_id) != 'nan' and str(movie_id).strip() != '':
            try:
                # Fetch movie details with credits appended to get director in one call
                tmdb_data = self.tmdb.fetch_movie_details(movie_id, append_to_response="credits")
                self.enrichment_stats['total_api_calls'] += 1
                
                # Extract credits data if present
                if 'credits' in tmdb_data:
                    credits_data = tmdb_data['credits']
                    
            except Exception as e:
                log_error(f"Failed to fetch with ID {movie_id}: {e}")
        
        # If ID fetch failed and we have title, try search
        if not tmdb_data and title and str(title).strip() != '':
            try:
                search_movie_id = self.find_movie_by_search(title, release_year)
                if search_movie_id:
                    tmdb_data = self.tmdb.fetch_movie_details(search_movie_id, append_to_response="credits")
                    self.enrichment_stats['total_api_calls'] += 1
                    
                    # Extract credits data if present
                    if 'credits' in tmdb_data:
                        credits_data = tmdb_data['credits']
                    
                    # Update the ID with the found one if it was missing
                    if self.is_field_missing(row.get('id'), 'id'):
                        row['id'] = search_movie_id
            except Exception as e:
                log_error(f"Search and fetch failed for '{title}': {e}")
        
        # Fill missing data from TMDb response
        if tmdb_data:
            enriched = False
            
            # Process each missing field
            for field in missing_fields:
                if field in ['budget', 'revenue', 'runtime', 'vote_count', 'popularity']:
                    # Direct numeric mapping
                    if field in tmdb_data and tmdb_data[field] is not None:
                        tmdb_value = tmdb_data[field]
                        # For budget and revenue, only use non-zero values
                        if field in ['budget', 'revenue']:
                            if tmdb_value and tmdb_value != 0:
                                row[field] = tmdb_value
                                enriched = True
                                log_info(f"Filled {field}: {tmdb_value}")
                        else:
                            # For other numeric fields, accept any value including 0
                            row[field] = tmdb_value
                            enriched = True
                            log_info(f"Filled {field}: {tmdb_value}")
                
                elif field in ['vote_average', 'rating']:
                    # Use vote_average for rating field
                    tmdb_field = 'vote_average'
                    if tmdb_field in tmdb_data and tmdb_data[tmdb_field] is not None:
                        tmdb_value = tmdb_data[tmdb_field]
                        if tmdb_value and tmdb_value > 0:  # Only use positive ratings
                            row[field] = tmdb_value
                            enriched = True
                            log_info(f"Filled {field}: {tmdb_value}")
                
                elif field in ['title', 'release_date', 'overview', 'tagline', 'status']:
                    # Direct text field mapping
                    if field in tmdb_data and tmdb_data[field] is not None:
                        tmdb_value = str(tmdb_data[field]).strip()
                        if tmdb_value and tmdb_value not in ['', 'null', 'None']:
                            row[field] = tmdb_value
                            enriched = True
                            log_info(f"Filled {field}: {tmdb_value}")
                
                elif field == 'director':
                    # Extract director from credits data
                    director_name = None
                    if credits_data and 'crew' in credits_data:
                        for crew_member in credits_data['crew']:
                            if crew_member.get('job') == 'Director':
                                director_name = crew_member.get('name')
                                break
                    
                    if director_name:
                        row[field] = director_name
                        enriched = True
                        log_info(f"Filled director: {director_name}")
                
                elif field in ['genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages']:
                    # List fields - convert to comma-separated strings
                    if field in tmdb_data and tmdb_data[field]:
                        tmdb_value = tmdb_data[field]
                        if isinstance(tmdb_value, list) and tmdb_value:
                            # Convert list to comma-separated string
                            if field == 'keywords':
                                # Keywords might have objects with 'name' field
                                if tmdb_value and isinstance(tmdb_value[0], dict):
                                    keyword_names = [kw.get('name', str(kw)) for kw in tmdb_value if kw.get('name')]
                                    if keyword_names:
                                        row[field] = ', '.join(keyword_names)
                                        enriched = True
                                        log_info(f"Filled {field}: {row[field]}")
                                else:
                                    row[field] = ', '.join(str(item) for item in tmdb_value)
                                    enriched = True
                                    log_info(f"Filled {field}: {row[field]}")
                            else:
                                # For other list fields, extract names if they're objects
                                if tmdb_value and isinstance(tmdb_value[0], dict):
                                    names = [item.get('name', str(item)) for item in tmdb_value if item.get('name')]
                                    if names:
                                        row[field] = ', '.join(names)
                                        enriched = True
                                        log_info(f"Filled {field}: {row[field]}")
                                else:
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
        """Enrich the movie dataset with ALL target columns (except id)"""
        log_info("Starting enrichment of movie dataset with checkpointing")
        log_info(f"Total rows to process: {len(df)}")
        log_info(f"Will check these columns for missing data: {', '.join([col for col in self.target_columns if col != 'id'])}")
        log_info(f"Checkpoint interval: {self.checkpoint_interval} rows")
        
        # Try to load from checkpoint
        enriched_rows, start_index = self.load_checkpoint()
        
        if enriched_rows is None:
            enriched_rows = []
            start_index = 0
            self.enrichment_stats['start_time'] = datetime.now()
        else:
            log_info(f"Resuming from row {start_index}")
        
        # Add missing target columns if they don't exist
        missing_columns = [col for col in self.target_columns if col not in df.columns]
        if missing_columns:
            log_info(f"Adding missing columns: {missing_columns}")
            for col in missing_columns:
                df[col] = ""
        
        # Process rows starting from checkpoint
        for index in range(start_index, len(df)):
            try:
                row = df.iloc[index]
                self.enrichment_stats['processed'] += 1
                enriched_row, was_enriched = self.enrich_movie_row(row.to_dict())
                
                if was_enriched:
                    self.enrichment_stats['enriched'] += 1
                    log_info(f"Successfully enriched row {index + 1}")
                
                enriched_rows.append(enriched_row)
                
                # Save checkpoint periodically
                if (index + 1) % self.checkpoint_interval == 0:
                    log_info(f"Progress: {index + 1}/{len(df)} rows processed")
                    completion_pct = ((index + 1) / len(df)) * 100
                    
                    # Update progress file with completion percentage
                    if os.path.exists(self.progress_file):
                        try:
                            with open(self.progress_file, 'r') as f:
                                progress_data = json.load(f)
                            progress_data['completion_percentage'] = round(completion_pct, 2)
                            with open(self.progress_file, 'w') as f:
                                json.dump(progress_data, f, indent=2)
                        except:
                            pass
                    
                    self.save_checkpoint(enriched_rows, index + 1)
                    
                    # Estimate time remaining
                    if self.enrichment_stats['start_time']:
                        elapsed = datetime.now() - self.enrichment_stats['start_time']
                        rate = (index + 1 - start_index) / elapsed.total_seconds()
                        remaining_rows = len(df) - (index + 1)
                        eta_seconds = remaining_rows / rate if rate > 0 else 0
                        eta = str(datetime.timedelta(seconds=int(eta_seconds)))
                        log_info(f"Processing rate: {rate:.2f} rows/sec, ETA: {eta}")
                
                # Small delay to respect API rate limits
                time.sleep(0.1)
                
            except Exception as e:
                log_error(f"Error processing row {index + 1}: {e}")
                self.enrichment_stats['failed'] += 1
                # Add the original row even if processing failed
                enriched_rows.append(row.to_dict())
        
        return pd.DataFrame(enriched_rows)
    
    def filter_output_columns(self, df):
        """Filter the dataset to only include the specified output columns"""
        log_info(f"Filtering dataset to include only: {', '.join(self.output_columns)}")
        
        # Add missing columns with empty values
        for col in self.output_columns:
            if col not in df.columns:
                df[col] = ""
                log_info(f"Added missing column: {col}")
        
        # Select only the desired columns in the specified order
        filtered_df = df[self.output_columns].copy()
        
        log_info(f"Dataset filtered from {len(df.columns)} to {len(filtered_df.columns)} columns")
        return filtered_df
    
    def print_enrichment_summary(self):
        """Print a summary of the enrichment process"""
        print("\n" + "="*70)
        print("DATA ENRICHMENT SUMMARY")
        print("="*70)
        print(f"Target columns checked: {', '.join([col for col in self.target_columns if col != 'id'])}")
        print(f"Output columns: {', '.join(self.output_columns)}")
        print(f"\nProcessed: {self.enrichment_stats['processed']} rows")
        print(f"Enriched:  {self.enrichment_stats['enriched']} rows")
        print(f"Failed:    {self.enrichment_stats['failed']} rows")
        
        if self.enrichment_stats['processed'] > 0:
            success_rate = (self.enrichment_stats['enriched'] / self.enrichment_stats['processed']) * 100
            print(f"Success:   {success_rate:.1f}%")
        
        print(f"\nTotal API calls made: {self.enrichment_stats['total_api_calls']}")
        
        if self.enrichment_stats['start_time']:
            total_time = datetime.now() - self.enrichment_stats['start_time']
            print(f"Total processing time: {total_time}")
            
            if self.enrichment_stats['processed'] > 0:
                rate = self.enrichment_stats['processed'] / total_time.total_seconds()
                print(f"Average processing rate: {rate:.2f} rows/second")
        
        print("="*70)

def main():
    """Main enrichment process for the specific dataset"""
    try:
        enricher = ModifiedDataEnrichment()
        
        log_info("Starting comprehensive data enrichment process...")
        start_time = datetime.now()
        
        # Define your input file path here
        BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
        ROOT_DIR = os.path.dirname(BASE_DIR) 
        MOVIES_MAIN_PATH = os.path.join(ROOT_DIR, "Dataset", "TMDB_movie_dataset_v11.csv")
        output_file_path = "TMDB_all_movies_enriched.csv"
        
        # Load the dataset 
        log_info(f"Loading dataset from {MOVIES_MAIN_PATH}...")
        df = read_csv(MOVIES_MAIN_PATH)
        
        log_info(f"Loaded {len(df)} rows from dataset")
        log_info(f"Dataset columns: {list(df.columns)}")
        
        # Check which target columns exist in the dataset
        existing_target_cols = [col for col in enricher.target_columns if col in df.columns]
        missing_target_cols = [col for col in enricher.target_columns if col not in df.columns]
        
        if missing_target_cols:
            log_info(f"These target columns are missing from dataset and will be added: {missing_target_cols}")
            # Add missing columns with empty values
            for col in missing_target_cols:
                df[col] = ""
        
        log_info(f"Will check these columns for missing data: {[col for col in enricher.target_columns if col != 'id']}")
        
        # Enrich the dataset
        enriched_df = enricher.enrich_dataset(df)
        
        # Filter to only desired output columns
        final_df = enricher.filter_output_columns(enriched_df)
        
        # Save enriched file
        log_info(f"Saving enriched dataset to {output_file_path}...")
        write_csv(final_df, output_file_path)
        
        # Calculate total time
        end_time = datetime.now()
        total_time = end_time - start_time
        
        # Clean up checkpoint files on successful completion
        enricher.cleanup_checkpoint_files()
        
        # Print summary
        enricher.print_enrichment_summary()
        print(f"\nTotal processing time: {total_time}")
        print(f"Enriched file saved: {output_file_path}")
        print(f"Final dataset shape: {final_df.shape}")
        print(f"Final columns: {list(final_df.columns)}")
        
        log_info("Comprehensive data enrichment completed successfully!")
        
    except Exception as e:
        log_error(f"Critical error in enrichment process: {e}")
        raise

if __name__ == "__main__":
    main()