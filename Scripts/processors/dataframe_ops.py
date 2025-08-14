import pandas as pd
from processors.data_cleaning import clean_text, clean_list_column, standardize_date_format

def remove_duplicates(df, subset_columns):
    """Remove duplicates and log the count"""
    original_count = len(df)
    df_clean = df.drop_duplicates(subset=subset_columns, keep='first')
    duplicates_removed = original_count - len(df_clean)
    
    from utils.logger import log_info
    if duplicates_removed > 0:
        log_info(f"Removed {duplicates_removed} duplicate rows based on {subset_columns}")
    
    return df_clean

def fill_missing_values(df, column, fetch_func, movie_id_column="id"):
    """Fill missing values using the fetch function"""
    from utils.logger import log_info
    
    missing_mask = df[column].isna() | (df[column] == "") | (df[column] == "0")
    missing_count = missing_mask.sum()
    
    if missing_count == 0:
        log_info(f"No missing values found in column '{column}'")
        return df
    
    log_info(f"Filling {missing_count} missing values in column '{column}'")
    filled_count = 0
    
    for idx in df[missing_mask].index:
        try:
            movie_id = df.at[idx, movie_id_column]
            fetched_data = fetch_func(movie_id)
            
            if fetched_data and column in fetched_data and fetched_data[column]:
                df.at[idx, column] = fetched_data[column]
                filled_count += 1
        except Exception as e:
            from utils.logger import log_error
            log_error(f"Error filling missing value for movie ID {movie_id}: {e}")
    
    log_info(f"Successfully filled {filled_count} out of {missing_count} missing values in '{column}'")
    return df

def clean_dataframe(df, text_columns=None, list_columns=None, date_columns=None):
    """Comprehensive dataframe cleaning"""
    from utils.logger import log_info
    
    # Clean text columns
    if text_columns:
        for col in text_columns:
            if col in df.columns:
                log_info(f"Cleaning text column: {col}")
                df[col] = df[col].apply(clean_text)
    
    # Clean list columns
    if list_columns:
        for col in list_columns:
            if col in df.columns:
                log_info(f"Cleaning list column: {col}")
                df[col] = df[col].apply(clean_list_column)
    
    # Clean and standardize date columns
    if date_columns:
        for col in date_columns:
            if col in df.columns:
                log_info(f"Standardizing date column: {col}")
                df[col] = df[col].apply(standardize_date_format)
    
    return df