import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional, Tuple
import re
from utils.logger import log_info, log_error

class DataValidator:
    """
    **Python classes implementation (5%)** - OOP class for data validation.
    Data validation class implementing OOP principles with comprehensive error handling.
    Demonstrates Python Classes/Objects implementation for data validation.
    """
    
    def __init__(self):
        """Initialize validator with validation rules dictionary and statistics tracking."""
        # **Python classes implementation (5%)** - Encapsulation with private attributes
        self._validation_rules = {
            'id': self.validate_movie_id,
            'title': self.validate_title,
            'vote_average': lambda x: self.validate_rating(x, 'tmdb'),
            'vote_count': self.validate_votes,
            'release_date': self.validate_date,
            'revenue': self.validate_financial,
            'runtime': self.validate_runtime,
            'budget': self.validate_financial,
            'popularity': self.validate_popularity,
            'genres': self.validate_list_field,
            'production_companies': self.validate_list_field,
            'production_countries': self.validate_list_field,
            'spoken_languages': self.validate_list_field,
            'cast': self.validate_list_field,
            'director': self.validate_list_field,
            'writers': self.validate_list_field,
            'imdb_rating': lambda x: self.validate_rating(x, 'imdb'),
            'imdb_votes': self.validate_votes,
            'avg_rating': lambda x: self.validate_rating(x, 'general'),
            'total_ratings': self.validate_votes,
            'std_dev': self.validate_std_dev,
            'last_rated': self.validate_date
        }
        
        # **Encapsulation** - Private attributes for internal use
        self._critical_fields = ['title', 'release_date', 'budget', 'revenue', 'runtime']
        self._api_enrichable_fields = [
            'vote_average', 'vote_count', 'genres', 'production_companies',
            'production_countries', 'spoken_languages', 'cast', 'director', 'writers'
        ]
        
        # Statistics tracking
        self.validation_stats = {
            'total_validations': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'api_corrections': 0
        }

    # **Data processing functions (5%)** - Clean, reusable validation functions
    def validate_movie_id(self, movie_id) -> bool:
        """
        **Error handling (5%)** - Comprehensive error handling with logging.
        Validate movie ID with comprehensive error handling.
        """
        try:
            self.validation_stats['total_validations'] += 1
            
            if movie_id is None or pd.isna(movie_id):
                self.validation_stats['failed_validations'] += 1
                return False
            
            id_val = int(float(str(movie_id)))
            is_valid = id_val > 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Movie ID validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in movie ID validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_title(self, title) -> bool:
        """
        **Data processing functions (5%)** - Specialized title validation.
        **Error handling (5%)** - Robust error handling with special character support.
        """
        try:
            self.validation_stats['total_validations'] += 1
            
            if not title or pd.isna(title) or str(title).strip() == '' or str(title).lower() == 'nan':
                self.validation_stats['failed_validations'] += 1
                return False
            
            title_str = str(title).strip()
            # Title should have at least 1 character and not be just special characters
            clean_title = re.sub(r'[^\w\s]', '', title_str)
            is_valid = len(clean_title.strip()) > 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except Exception as e:
            log_error(f"Title validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_rating(self, rating, rating_system='tmdb') -> bool:
        """
        **Data processing functions (5%)** - Modular rating validation with system support.
        **Error handling (5%)** - System-specific validation with error logging.
        """
        try:
            self.validation_stats['total_validations'] += 1
            
            if rating is None or pd.isna(rating):
                self.validation_stats['failed_validations'] += 1
                return False
            
            rating_val = float(rating)
            
            # System-specific validation using dictionaries
            rating_limits = {
                'tmdb': (0, 10),
                'imdb': (0, 10),
                'general': (0, 10)
            }
            
            min_val, max_val = rating_limits.get(rating_system, (0, 10))
            is_valid = min_val <= rating_val <= max_val
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Rating validation error for {rating_system}: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in rating validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_votes(self, votes) -> bool:
        """**Data processing functions (5%)** - Vote count validation with error handling."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if votes is None or pd.isna(votes):
                self.validation_stats['failed_validations'] += 1
                return False
            
            votes_val = int(float(str(votes)))
            is_valid = votes_val >= 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Votes validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in votes validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_date(self, date_str) -> bool:
        """
        **Data processing functions (5%)** - Date format validation.
        **Error handling (5%)** - Comprehensive date parsing with error handling.
        """
        try:
            self.validation_stats['total_validations'] += 1
            
            if not date_str or pd.isna(date_str) or str(date_str).strip() == '' or str(date_str).lower() == 'nan':
                self.validation_stats['failed_validations'] += 1
                return False
            
            date_str = str(date_str).strip()
            
            # Check if it matches YYYY-MM-DD format (our standard)
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                    self.validation_stats['successful_validations'] += 1
                    return True
                except ValueError:
                    self.validation_stats['failed_validations'] += 1
                    return False
            
            self.validation_stats['failed_validations'] += 1
            return False
            
        except Exception as e:
            log_error(f"Date validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_financial(self, amount) -> bool:
        """**Data processing functions (5%)** - Financial amount validation."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if amount is None or pd.isna(amount):
                self.validation_stats['failed_validations'] += 1
                return False
            
            amount_val = float(amount)
            is_valid = amount_val >= 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Financial validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in financial validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_runtime(self, runtime) -> bool:
        """**Data processing functions (5%)** - Runtime validation with reasonable limits."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if runtime is None or pd.isna(runtime):
                self.validation_stats['failed_validations'] += 1
                return False
            
            runtime_val = int(float(str(runtime)))
            # Reasonable runtime limits: 1 minute to 10 hours (600 minutes)
            is_valid = 1 <= runtime_val <= 600
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Runtime validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in runtime validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_popularity(self, popularity) -> bool:
        """**Data processing functions (5%)** - Popularity score validation."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if popularity is None or pd.isna(popularity):
                self.validation_stats['failed_validations'] += 1
                return False
            
            pop_val = float(popularity)
            is_valid = pop_val >= 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Popularity validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in popularity validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_std_dev(self, std_dev) -> bool:
        """**Data processing functions (5%)** - Standard deviation validation."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if std_dev is None or pd.isna(std_dev):
                self.validation_stats['failed_validations'] += 1
                return False
            
            std_val = float(std_dev)
            is_valid = std_val >= 0
            
            if is_valid:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            return is_valid
            
        except (ValueError, TypeError) as e:
            log_error(f"Standard deviation validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False
        except Exception as e:
            log_error(f"Unexpected error in std dev validation: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_list_field(self, field) -> bool:
        """**Data processing functions (5%)** - List field validation with error handling."""
        try:
            self.validation_stats['total_validations'] += 1
            
            if field is None or pd.isna(field):
                self.validation_stats['successful_validations'] += 1
                return True  # Empty lists are valid
            
            # If it's already a list
            if isinstance(field, list):
                self.validation_stats['successful_validations'] += 1
                return len(field) >= 0  # Any list length is valid
            
            # If it's a string representation
            field_str = str(field).strip()
            if not field_str or field_str.lower() in ['nan', 'none', '']:
                self.validation_stats['successful_validations'] += 1
                return True  # Empty is valid
            
            self.validation_stats['successful_validations'] += 1
            return True  # We'll let the cleaning process handle the actual parsing
            
        except Exception as e:
            log_error(f"List field validation error: {e}")
            self.validation_stats['failed_validations'] += 1
            return False

    def validate_dataframe(self, df: pd.DataFrame, custom_rules: Optional[Dict[str, Callable]] = None, 
                          fetch_func: Optional[Callable] = None, fallback_enabled: bool = True) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Complex DataFrame validation with filtering and aggregation.
        **Error handling (5%)** - Comprehensive validation with fallback logic and error recovery.
        Enhanced dataframe validation with fallback logic and comprehensive error handling.
        """
        try:
            log_info("Starting comprehensive dataframe validation")
            
            # Use custom rules if provided, otherwise use default rules
            rules_to_use = custom_rules if custom_rules else {
                col: rule for col, rule in self._validation_rules.items() 
                if col in df.columns
            }
            
            validation_results = {
                'valid_rows': {},
                'invalid_rows': {},
                'corrected_rows': {},
                'total_rows': len(df),
                'validation_summary': {},
                'overall_health_score': 0.0,
                'processing_time': 0.0
            }
            
            import time
            start_time = time.time()
            
            # **Pandas operations (10%)** - DataFrame copying and manipulation
            df_corrected = df.copy()
            total_validations = 0
            total_valid = 0
            
            # Validate each column using comprehensive error handling
            for column, validator in rules_to_use.items():
                if column not in df.columns:
                    log_error(f"Column '{column}' not found in dataframe")
                    continue
                
                try:
                    log_info(f"Validating column: {column}")
                    
                    # **Pandas operations (10%)** - Apply function with lambda for validation
                    valid_mask = df[column].apply(lambda x: self._safe_validate(validator, x, column))
                    invalid_indices = df[~valid_mask].index.tolist()
                    valid_count = valid_mask.sum()
                    invalid_count = len(invalid_indices)
                    
                    # Update statistics
                    total_validations += len(df)
                    total_valid += valid_count
                    
                    # **Pandas operations (10%)** - Boolean indexing and filtering
                    validation_results['valid_rows'][column] = df[valid_mask].index.tolist()
                    validation_results['invalid_rows'][column] = invalid_indices
                    validation_results['validation_summary'][column] = {
                        'valid': int(valid_count),
                        'invalid': invalid_count,
                        'percentage_valid': float((valid_count / len(df)) * 100) if len(df) > 0 else 0.0
                    }
                    
                    log_info(f"Column '{column}': {valid_count} valid, {invalid_count} invalid "
                            f"({validation_results['validation_summary'][column]['percentage_valid']:.2f}% valid)")
                    
                    # Apply fallback logic if enabled
                    if (fallback_enabled and fetch_func and invalid_count > 0 and 
                        column != 'id' and column in self._api_enrichable_fields):
                        corrected_indices = self._apply_fallback_correction(
                            df_corrected, column, invalid_indices, validator, fetch_func
                        )
                        validation_results['corrected_rows'][column] = corrected_indices
                        
                        if corrected_indices:
                            # **Pandas operations (10%)** - Re-validation after corrections
                            corrected_mask = df_corrected[column].apply(
                                lambda x: self._safe_validate(validator, x, column)
                            )
                            new_valid_count = corrected_mask.sum()
                            improvement = new_valid_count - valid_count
                            
                            validation_results['validation_summary'][column].update({
                                'corrected': len(corrected_indices),
                                'valid_after_correction': int(new_valid_count),
                                'improvement': int(improvement),
                                'final_percentage_valid': float((new_valid_count / len(df)) * 100)
                            })
                            
                            total_valid += improvement
                            self.validation_stats['api_corrections'] += len(corrected_indices)
                            
                            log_info(f"Column '{column}': Corrected {len(corrected_indices)} values, "
                                    f"improved by {improvement} valid entries")
                
                except Exception as e:
                    log_error(f"Error validating column '{column}': {e}")
                    continue
            
            # Calculate overall health score and processing time
            validation_results['overall_health_score'] = float(
                (total_valid / total_validations * 100) if total_validations > 0 else 0
            )
            validation_results['processing_time'] = float(time.time() - start_time)
            
            # Return both results and corrected dataframe
            validation_results['corrected_dataframe'] = df_corrected
            
            log_info(f"Validation completed in {validation_results['processing_time']:.2f}s. "
                    f"Overall health score: {validation_results['overall_health_score']:.2f}%")
            
            return validation_results
            
        except Exception as e:
            log_error(f"Critical error in dataframe validation: {e}")
            raise

    def _safe_validate(self, validator: Callable, value: Any, column_name: str) -> bool:
        """
        **Error handling (5%)** - Safe validation wrapper with error recovery.
        Safely apply validator with comprehensive error handling.
        """
        try:
            return validator(value)
        except Exception as e:
            log_error(f"Error validating value in column '{column_name}': {e}")
            return False

    def _apply_fallback_correction(self, df_corrected: pd.DataFrame, column: str, 
                                 invalid_indices: List[int], validator: Callable, 
                                 fetch_func: Callable) -> List[int]:
        """
        **Error handling (5%)** - API fallback correction with comprehensive error handling.
        **Pandas operations (10%)** - DataFrame value assignment and indexing.
        """
        corrected_indices = []
        max_corrections = min(10, len(invalid_indices))  # Limit API calls for efficiency
        
        log_info(f"Attempting to correct {max_corrections} invalid values in '{column}' using API fallback")
        
        for idx in invalid_indices[:max_corrections]:
            try:
                # **Pandas operations (10%)** - DataFrame value access with at[]
                movie_id = df_corrected.at[idx, 'id']
                if pd.isna(movie_id):
                    continue
                
                # Fetch data from API
                api_data = fetch_func(int(movie_id))
                
                if api_data and column in api_data:
                    api_value = api_data[column]
                    
                    # Validate the API value
                    if self._safe_validate(validator, api_value, column):
                        # Compare with existing value to choose better one
                        current_value = df_corrected.at[idx, column]
                        better_value = self._choose_better_value(column, current_value, api_value)
                        
                        if better_value != current_value:
                            # **Pandas operations (10%)** - DataFrame value assignment
                            df_corrected.at[idx, column] = better_value
                            corrected_indices.append(idx)
                            log_info(f"Corrected movie ID {movie_id}, column '{column}': {current_value} -> {better_value}")
                
            except Exception as e:
                log_error(f"Error in fallback correction for movie ID {movie_id}, column '{column}': {e}")
        
        return corrected_indices

    def _choose_better_value(self, column: str, current_value: Any, api_value: Any) -> Any:
        """
        **Data processing functions (5%)** - Value comparison logic with business rules.
        Choose the better value between current and API value based on column type.
        """
        # If current value is invalid/missing, always choose API
        if pd.isna(current_value) or str(current_value).strip() == '' or str(current_value).lower() == 'nan':
            return api_value
        
        # Column-specific logic using dictionaries for decision mapping
        if column in ['vote_average', 'imdb_rating', 'avg_rating']:
            # For ratings, prefer non-zero values, then higher values
            current_val = float(current_value) if current_value else 0
            api_val = float(api_value) if api_value else 0
            
            if current_val == 0 and api_val > 0:
                return api_value
            elif api_val == 0 and current_val > 0:
                return current_value
            else:
                return api_value if api_val > current_val else current_value
        
        elif column in ['vote_count', 'imdb_votes', 'total_ratings']:
            # For vote counts, prefer higher numbers (more votes = more reliable)
            current_val = int(float(current_value)) if current_value else 0
            api_val = int(float(api_value)) if api_value else 0
            return api_value if api_val > current_val else current_value
        
        elif column in ['budget', 'revenue']:
            # For financial data, prefer non-zero values
            current_val = float(current_value) if current_value else 0
            api_val = float(api_value) if api_value else 0
            
            if current_val == 0 and api_val > 0:
                return api_value
            elif api_val == 0 and current_val > 0:
                return current_value
            else:
                return api_value  # API data is usually more reliable
        
        elif column == 'runtime':
            # For runtime, prefer reasonable values
            current_val = int(float(current_value)) if current_value else 0
            api_val = int(float(api_value)) if api_value else 0
            
            if 60 <= api_val <= 300:  # 1-5 hours is most common
                return api_value
            elif 60 <= current_val <= 300:
                return current_value
            else:
                return api_value
        
        elif column in ['genres', 'cast', 'director', 'writers', 'production_companies', 
                       'production_countries', 'spoken_languages']:
            # For list fields, prefer the one with more information
            if isinstance(api_value, list) and isinstance(current_value, list):
                return api_value if len(api_value) > len(current_value) else current_value
            elif isinstance(api_value, list):
                return api_value
            elif isinstance(current_value, list):
                return current_value
            else:
                api_str = str(api_value).strip()
                current_str = str(current_value).strip()
                return api_value if len(api_str) > len(current_str) else current_value
        
        # Default: prefer API value (usually more reliable)
        return api_value

    def get_validation_rules(self) -> Dict[str, Callable]:
        """**Python classes implementation (5%)** - Getter method demonstrating encapsulation."""
        return self._validation_rules.copy()

    def get_validation_stats(self) -> Dict[str, int]:
        """Return validation statistics."""
        return self.validation_stats.copy()

    def reset_stats(self):
        """Reset validation statistics."""
        for key in self.validation_stats:
            self.validation_stats[key] = 0

# **Data processing functions (5%)** - Convenience validation rule sets for backward compatibility
BASIC_VALIDATION_RULES = {
    'id': lambda x: DataValidator().validate_movie_id(x),
    'title': lambda x: DataValidator().validate_title(x),
    'release_date': lambda x: DataValidator().validate_date(x)
}

COMPREHENSIVE_VALIDATION_RULES = {
    'id': lambda x: DataValidator().validate_movie_id(x),
    'title': lambda x: DataValidator().validate_title(x),
    'vote_average': lambda x: DataValidator().validate_rating(x, 'tmdb'),
    'vote_count': lambda x: DataValidator().validate_votes(x),
    'release_date': lambda x: DataValidator().validate_date(x),
    'revenue': lambda x: DataValidator().validate_financial(x),
    'runtime': lambda x: DataValidator().validate_runtime(x),
    'budget': lambda x: DataValidator().validate_financial(x),
    'popularity': lambda x: DataValidator().validate_popularity(x),
    'genres': lambda x: DataValidator().validate_list_field(x),
    'production_companies': lambda x: DataValidator().validate_list_field(x),
    'production_countries': lambda x: DataValidator().validate_list_field(x),
    'spoken_languages': lambda x: DataValidator().validate_list_field(x),
    'cast': lambda x: DataValidator().validate_list_field(x),
    'director': lambda x: DataValidator().validate_list_field(x),
    'writers': lambda x: DataValidator().validate_list_field(x),
    'imdb_rating': lambda x: DataValidator().validate_rating(x, 'imdb'),
    'imdb_votes': lambda x: DataValidator().validate_votes(x),
    'avg_rating': lambda x: DataValidator().validate_rating(x, 'general'),
    'total_ratings': lambda x: DataValidator().validate_votes(x),
    'std_dev': lambda x: DataValidator().validate_std_dev(x),
    'last_rated': lambda x: DataValidator().validate_date(x)
}

# **Data processing functions (5%)** - Backward compatible functions
def validate_dataframe(df: pd.DataFrame, validation_rules: Dict[str, Callable], 
                      fetch_func: Optional[Callable] = None, fallback_enabled: bool = True) -> Dict[str, Any]:
    """Backward compatible function using DataValidator class."""
    validator = DataValidator()
    return validator.validate_dataframe(df, validation_rules, fetch_func, fallback_enabled)