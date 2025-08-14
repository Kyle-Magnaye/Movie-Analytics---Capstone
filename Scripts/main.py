import pandas as pd
from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.data_cleaning import clean_text, clean_list_column, standardize_date_format
from processors.tmdb_fetcher import fetch_movie_details
from processors.dataframe_ops import (
    remove_duplicates, fill_missing_values, clean_dataframe
)
from utils.logger import log_info, log_error
from utils.validators import (
    validate_budget, validate_revenue, validate_date, validate_rating, 
    validate_movie_id, validate_dataframe
)
import json

def process_movies_main(df):
    """Process the main movies dataframe"""
    log_info("Processing movies_main.csv")
    
    # Step 1: Remove duplicates
    df = remove_duplicates(df, ["id"])
    
    # Step 2: Fill missing values from TMDb API
    critical_columns = ["budget", "revenue", "title", "release_date"]
    
    for column in critical_columns:
        if column in df.columns:
            df = fill_missing_values(
                df, column, 
                lambda mid: fetch_movie_details(mid)
            )
    
    # Step 3: Clean the data
    text_columns = ["title", "overview", "tagline"] if "overview" in df.columns else ["title"]
    date_columns = ["release_date"]
    
    df = clean_dataframe(df, text_columns=text_columns, date_columns=date_columns)
    
    # Step 4: Validate and fix invalid data
    validation_rules = {
        "id": validate_movie_id,
        "budget": validate_budget,
        "revenue": validate_revenue,
        "release_date": validate_date
    }
    
    if "vote_average" in df.columns:
        validation_rules["vote_average"] = validate_rating
    
    validation_results = validate_dataframe(df, validation_rules)
    
    # Fix invalid data by re-fetching from TMDb
    for column, invalid_indices in validation_results['invalid_rows'].items():
        if invalid_indices and column in critical_columns:
            log_info(f"Re-fetching invalid data for column '{column}'")
            for idx in invalid_indices:
                try:
                    movie_id = df.at[idx, "id"]
                    fetched_data = fetch_movie_details(movie_id)
                    if fetched_data and column in fetched_data:
                        df.at[idx, column] = fetched_data[column]
                except Exception as e:
                    log_error(f"Error re-fetching data for movie ID {movie_id}: {e}")
    
    return df

def process_movie_extended(df):
    """Process the extended movies dataframe"""
    log_info("Processing movie_extended.csv")
    
    # Step 1: Remove duplicates
    df = remove_duplicates(df, ["id"])
    
    # Step 2: Fill missing values from TMDb API
    list_columns_to_fetch = ["genres", "production_companies", "production_countries", "spoken_languages"]
    
    for column in list_columns_to_fetch:
        if column in df.columns:
            df = fill_missing_values(
                df, column,
                lambda mid: fetch_movie_details(mid)
            )
    
    # Step 3: Clean the data
    list_columns = ["genres", "production_companies", "production_countries", "spoken_languages"]
    text_columns = ["homepage"] if "homepage" in df.columns else []
    
    df = clean_dataframe(df, text_columns=text_columns, list_columns=list_columns)
    
    # Step 4: Basic validation
    validation_rules = {"id": validate_movie_id}
    validate_dataframe(df, validation_rules)
    
    return df

def process_ratings(data):
    """Process the ratings JSON data"""
    log_info("Processing ratings.json")
    
    if not isinstance(data, dict):
        log_error("Ratings data is not in expected dictionary format")
        return data
    
    processed_data = {}
    
    for movie_id, rating_info in data.items():
        try:
            # Clean and validate movie_id
            clean_movie_id = str(movie_id).strip()
            
            if isinstance(rating_info, dict):
                cleaned_rating = {}
                
                # Clean numeric fields
                for field in ["avg_rating", "total_ratings", "std_dev"]:
                    if field in rating_info:
                        value = rating_info[field]
                        if value is not None and str(value).strip() != '':
                            try:
                                if field == "total_ratings":
                                    cleaned_rating[field] = int(float(value))
                                else:
                                    cleaned_rating[field] = float(value)
                            except (ValueError, TypeError):
                                cleaned_rating[field] = None
                        else:
                            cleaned_rating[field] = None
                
                # Clean date field
                if "last_rated" in rating_info:
                    cleaned_rating["last_rated"] = standardize_date_format(rating_info["last_rated"])
                
                processed_data[clean_movie_id] = cleaned_rating
            else:
                log_error(f"Invalid rating format for movie_id {movie_id}")
                
        except Exception as e:
            log_error(f"Error processing rating for movie_id {movie_id}: {e}")
    
    return processed_data

def main():
    """Main processing function"""
    try:
        log_info("Starting enhanced data cleaning process")
        
        # Step 1: Load the data
        log_info("Loading data files...")
        movies_main = read_csv("movies_main.csv")
        movie_extended = read_csv("movie_extended.csv")
        ratings = read_json("ratings.json")
        
        log_info(f"Loaded {len(movies_main)} rows from movies_main.csv")
        log_info(f"Loaded {len(movie_extended)} rows from movie_extended.csv")
        log_info(f"Loaded {len(ratings)} entries from ratings.json")
        
        # Step 2-5: Process each dataset
        movies_main_clean = process_movies_main(movies_main)
        movie_extended_clean = process_movie_extended(movie_extended)
        ratings_clean = process_ratings(ratings)
        
        # Step 6: Save cleaned files
        log_info("Saving cleaned files...")
        write_csv(movies_main_clean, "movies_main_clean.csv")
        write_csv(movie_extended_clean, "movie_extended_clean.csv")
        write_json(ratings_clean, "ratings_clean.json")
        
        # Final summary
        log_info("="*50)
        log_info("DATA CLEANING COMPLETED SUCCESSFULLY")
        log_info(f"Movies Main: {len(movies_main_clean)} rows")
        log_info(f"Movie Extended: {len(movie_extended_clean)} rows")
        log_info(f"Ratings: {len(ratings_clean)} entries")
        log_info("="*50)
        
    except Exception as e:
        log_error(f"Critical error in main process: {e}")
        raise

if __name__ == "__main__":
    main()