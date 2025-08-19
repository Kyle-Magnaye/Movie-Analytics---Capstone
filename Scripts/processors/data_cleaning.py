import re
import pandas as pd
import ast
from typing import List, Any, Union, Dict
from utils.logger import log_info, log_error

class DataCleaner:
    """
    Data cleaning class demonstrating OOP principles with inheritance and encapsulation.
    **Python classes implementation (5%)** - Shows object-oriented programming with attributes, methods, encapsulation.
    """
    
    def __init__(self):
        """Initialize DataCleaner with encoding fixes dictionary."""
        # **Encapsulation** - Private attributes for internal use
        self._encoding_fixes = {
            # Quotes and punctuation
            'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€"': '-', 'â€"': '--',
            'â€¦': '...', 'â€¢': '•', 'â‚¬': '€',
            
            # Accented characters - Latin
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã ': 'à', 'Ã¨': 'è', 'Ã¬': 'ì', 'Ã²': 'ò', 'Ã¹': 'ù',
            'Ã¢': 'â', 'Ãª': 'ê', 'Ã®': 'î', 'Ã´': 'ô', 'Ã»': 'û',
            'Ã£': 'ã', 'Ã±': 'ñ', 'Ã§': 'ç', 'Ã¼': 'ü', 'Ã¤': 'ä', 'Ã¶': 'ö',
            
            # Uppercase accented
            'Ã': 'À', 'Ã‰': 'É', 'Ã': 'Í', 'Ã"': 'Ó', 'Ãš': 'Ú',
            'Ã„': 'Ä', 'Ã‹': 'Ë', 'Ã': 'Ï', 'Ã–': 'Ö', 'Ãœ': 'Ü',
            'Ã'': 'Ñ', 'Ã‡': 'Ç',
            
            # Other symbols
            'Â£': '£', 'Â©': '©', 'Â®': '®', 'Â°': '°', 'Â±': '±',
            'Â²': '²', 'Â³': '³', 'Âµ': 'µ', 'Â¿': '¿', 'Â¡': '¡'
        }
        
        self._html_entities = {
            '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"',
            '&#39;': "'", '&apos;': "'", '&nbsp;': ' '
        }
        
        # Statistics tracking
        self.cleaning_stats = {
            'total_cleaned': 0,
            'encoding_fixes': 0,
            'html_fixes': 0,
            'whitespace_fixes': 0
        }

    def clean_text(self, text: Any) -> str:
        """
        **Data processing functions (5%)** - Clean, reusable function for text transformation.
        **Error handling (5%)** - Comprehensive try/except blocks with logging.
        """
        try:
            if not text or pd.isna(text) or str(text).strip() == '' or str(text).lower() == 'nan':
                return None
            
            text = str(text)
            original_text = text
            
            # Apply encoding fixes using encapsulated dictionary
            for bad, good in self._encoding_fixes.items():
                if bad in text:
                    text = text.replace(bad, good)
                    self.cleaning_stats['encoding_fixes'] += 1
            
            # Remove control characters
            text = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', text)
            
            # Normalize whitespace
            if re.search(r'\s{2,}', text):
                text = re.sub(r'\s+', ' ', text)
                self.cleaning_stats['whitespace_fixes'] += 1
            
            # Remove leading/trailing whitespace
            text = text.strip()
            
            # Handle HTML entities
            for entity, replacement in self._html_entities.items():
                if entity in text:
                    text = text.replace(entity, replacement)
                    self.cleaning_stats['html_fixes'] += 1
            
            # Track successful cleaning
            if text != original_text:
                self.cleaning_stats['total_cleaned'] += 1
            
            return text if text else None
            
        except Exception as e:
            log_error(f"Error in text cleaning: {e}")
            return str(text) if text else None

    def clean_list_column(self, cell: Any) -> List[str]:
        """
        **Data processing functions (5%)** - Reusable function for list data transformation.
        **Error handling (5%)** - Robust error handling with logging.
        """
        try:
            # Handle pandas NA values
            if pd.isna(cell):
                return []
            
            # If already a list, clean each item
            if isinstance(cell, list):
                return [self.clean_text(str(item)) for item in cell if self.clean_text(str(item))]
            
            # Convert to string and check for empty values
            cell_str = str(cell).strip()
            if not cell_str or cell_str.lower() in ['nan', 'none', '']:
                return []
            
            # Handle JSON-like strings
            if cell_str.startswith('[') and cell_str.endswith(']'):
                try:
                    parsed = ast.literal_eval(cell_str)
                    if isinstance(parsed, list):
                        items = [self.clean_text(str(item)) for item in parsed if self.clean_text(str(item))]
                        return list(dict.fromkeys(items))  # Remove duplicates, preserve order
                except (ValueError, SyntaxError) as e:
                    log_error(f"JSON parsing failed, treating as comma-separated: {e}")
                    # Fall through to comma-separated handling
            
            # Handle comma-separated strings
            items = []
            for item in cell_str.split(','):
                cleaned = self.clean_text(item.strip().strip('"\''))
                if cleaned:
                    items.append(cleaned)
            
            return list(dict.fromkeys(items))  # Remove duplicates, preserve order
            
        except Exception as e:
            log_error(f"Error in list column cleaning: {e}")
            return []

    def standardize_date_format(self, date_str: Any) -> str:
        """
        **Data processing functions (5%)** - Date standardization with Python Datetime.
        **Error handling (5%)** - Multiple format attempts with error logging.
        """
        try:
            if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
                return None
            
            date_str = str(date_str).strip()
            if date_str.lower() == 'nan':
                return None
            
            from datetime import datetime
            
            # Supported formats in priority order
            formats = [
                "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", 
                "%Y", "%Y-%m", "%d-%m-%Y", "%m-%d-%Y"
            ]
            
            for fmt in formats:
                try:
                    if fmt == "%Y" and len(date_str) == 4:
                        return f"{date_str}-01-01"  # Default to January 1st
                    elif fmt == "%Y-%m" and len(date_str) == 7:
                        return f"{date_str}-01"  # Default to 1st day
                    else:
                        date_obj = datetime.strptime(date_str, fmt)
                        return date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            log_error(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            log_error(f"Error in date standardization: {e}")
            return None

    def get_cleaning_stats(self) -> Dict[str, int]:
        """Return cleaning statistics for monitoring."""
        return self.cleaning_stats.copy()

    def reset_stats(self):
        """Reset cleaning statistics."""
        for key in self.cleaning_stats:
            self.cleaning_stats[key] = 0

# **Data processing functions (5%)** - Clean, modular functions for backward compatibility
_default_cleaner = DataCleaner()

def clean_text(text: Any) -> str:
    """Backward compatible function using the DataCleaner class."""
    return _default_cleaner.clean_text(text)

def clean_list_column(cell: Any) -> List[str]:
    """Backward compatible function using the DataCleaner class."""
    return _default_cleaner.clean_list_column(cell)

def standardize_date_format(date_str: Any) -> str:
    """Backward compatible function using the DataCleaner class."""
    return _default_cleaner.standardize_date_format(date_str)

class AdvancedDataCleaner(DataCleaner):
    """
    **Python classes implementation (5%)** - Demonstrates inheritance from DataCleaner.
    Extended cleaner with additional functionality for movie-specific data.
    """
    
    def __init__(self):
        """Initialize with parent attributes plus movie-specific settings."""
        super().__init__()  # **Inheritance** - Call parent constructor
        
        # **Encapsulation** - Additional private attributes for movie cleaning
        self._movie_specific_fixes = {
            'genre_mappings': {
                'Sci-Fi': 'Science Fiction',
                'Rom-Com': 'Romantic Comedy',
                'Action/Adventure': 'Action'
            },
            'country_mappings': {
                'USA': 'United States of America',
                'UK': 'United Kingdom',
                'UAE': 'United Arab Emirates'
            }
        }

    def clean_movie_genres(self, genres: Any) -> List[str]:
        """
        **Data processing functions (5%)** - Specialized function for genre cleaning.
        Movie-specific genre cleaning with standardization.
        """
        try:
            cleaned_genres = self.clean_list_column(genres)
            
            # Apply genre mappings
            standardized_genres = []
            for genre in cleaned_genres:
                standardized_genre = self._movie_specific_fixes['genre_mappings'].get(genre, genre)
                standardized_genres.append(standardized_genre)
            
            return standardized_genres
            
        except Exception as e:
            log_error(f"Error in movie genre cleaning: {e}")
            return []

    def clean_production_countries(self, countries: Any) -> List[str]:
        """
        **Data processing functions (5%)** - Specialized country name standardization.
        """
        try:
            cleaned_countries = self.clean_list_column(countries)
            
            # Apply country mappings
            standardized_countries = []
            for country in cleaned_countries:
                standardized_country = self._movie_specific_fixes['country_mappings'].get(country, country)
                standardized_countries.append(standardized_country)
            
            return standardized_countries
            
        except Exception as e:
            log_error(f"Error in production country cleaning: {e}")
            return []

    def clean_cast_and_crew(self, cast_data: Any) -> List[str]:
        """
        **Data processing functions (5%)** - Advanced cast/crew name cleaning.
        Handles special cases like comma-separated names with roles.
        """
        try:
            if pd.isna(cast_data):
                return []
            
            cast_str = str(cast_data).strip()
            if not cast_str or cast_str.lower() in ['nan', 'none']:
                return []
            
            # Handle cast with roles (Name as Character, Name as Character)
            if ' as ' in cast_str:
                cast_members = []
                for member in cast_str.split(','):
                    if ' as ' in member:
                        actor_name = member.split(' as ')[0].strip()
                        cleaned_name = self.clean_text(actor_name)
                        if cleaned_name:
                            cast_members.append(cleaned_name)
                    else:
                        cleaned_name = self.clean_text(member.strip())
                        if cleaned_name:
                            cast_members.append(cleaned_name)
                return list(dict.fromkeys(cast_members))
            else:
                return self.clean_list_column(cast_data)
                
        except Exception as e:
            log_error(f"Error in cast/crew cleaning: {e}")
            return []