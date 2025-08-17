import re
import pandas as pd
from typing import List, Any

def clean_text(text):
    """Enhanced text cleaning with special character handling"""
    if not text or pd.isna(text) or str(text).strip() == '' or str(text).lower() == 'nan':
        return None
    
    text = str(text)
    
    # Remove control characters and excessive whitespace
    text = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', text)
    text = re.sub(r'\s+', ' ', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Handle common encoding issues
    replacements = {
        'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€"': '-', 'â€"': '--',
        'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
        'Ã ': 'à', 'Ã¨': 'è', 'Ã¬': 'ì', 'Ã²': 'ò', 'Ã¹': 'ù'
    }
    
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    
    return text if text else None

def clean_list_column(cell):
    """Enhanced list column cleaning"""
    # Handle pandas NA values and empty cells safely
    try:
        if pd.isna(cell):
            return []
    except (ValueError, TypeError):
        # If pd.isna() fails, cell might be an array or other complex type
        pass
    
    # Convert to string and check for empty values
    cell_str = str(cell).strip()
    if not cell_str or cell_str == '' or cell_str.lower() == 'nan' or cell_str.lower() == 'none':
        return []
    
    # Handle JSON-like strings
    if cell_str.startswith('[') and cell_str.endswith(']'):
        try:
            import ast
            parsed = ast.literal_eval(cell_str)
            if isinstance(parsed, list):
                items = [clean_text(str(item)) for item in parsed if str(item).strip()]
                return list(filter(None, set(items)))
        except:
            pass
    
    # Handle comma-separated strings
    items = []
    for item in cell_str.split(','):
        cleaned = clean_text(item)
        if cleaned:
            items.append(cleaned)
    
    return list(set(items))  # Remove duplicates

def standardize_date_format(date_str):
    """Standardize all dates to YYYY-MM-DD format"""
    if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%Y"]
    
    from datetime import datetime
    for fmt in formats:
        try:
            if fmt == "%Y" and len(date_str) == 4:
                return f"{date_str}-01-01"  # Default to January 1st for year-only dates
            elif fmt != "%Y":
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    return None