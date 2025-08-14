import pandas as pd
from datetime import datetime
from typing import Any, Dict, List

def validate_budget(budget):
    """Validate budget values"""
    if budget is None:
        return True  # Allow None
    try:
        budget_float = float(budget)
        return budget_float >= 0
    except (ValueError, TypeError):
        return False

def validate_revenue(revenue):
    """Validate revenue values"""
    if revenue is None:
        return True  # Allow None
    try:
        revenue_float = float(revenue)
        return revenue_float >= 0
    except (ValueError, TypeError):
        return False

def validate_date(date_str):
    """Validate date format (should be YYYY-MM-DD after cleaning)"""
    if not date_str or pd.isna(date_str):
        return True  # Allow None/empty
    
    try:
        datetime.strptime(str(date_str), "%Y-%m-%d")
        return True
    except ValueError:
        return False

def validate_rating(rating):
    """Validate rating values (typically 0-10)"""
    if rating is None:
        return True  # Allow None
    try:
        rating_float = float(rating)
        return 0 <= rating_float <= 10
    except (ValueError, TypeError):
        return False

def validate_movie_id(movie_id):
    """Validate movie ID"""
    if movie_id is None:
        return False
    try:
        int(movie_id)
        return True
    except (ValueError, TypeError):
        return False

def validate_dataframe(df, validation_rules):
    """Validate entire dataframe and return validation results"""
    from utils.logger import log_info, log_error
    
    validation_results = {
        'total_rows': len(df),
        'invalid_rows': {},
        'summary': {}
    }
    
    for column, validator in validation_rules.items():
        if column not in df.columns:
            continue
            
        invalid_mask = ~df[column].apply(validator)
        invalid_count = invalid_mask.sum()
        
        validation_results['invalid_rows'][column] = df[invalid_mask].index.tolist()
        validation_results['summary'][column] = {
            'total': len(df),
            'invalid': invalid_count,
            'valid_percentage': ((len(df) - invalid_count) / len(df)) * 100
        }
        
        if invalid_count > 0:
            log_error(f"Column '{column}': {invalid_count} invalid values found")
        else:
            log_info(f"Column '{column}': All values are valid")
    
    return validation_results