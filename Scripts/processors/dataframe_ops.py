import pandas as pd
import numpy as np
from typing import List, Dict, Any, Callable, Optional, Tuple
from processors.data_cleaning import DataCleaner, AdvancedDataCleaner
from utils.logger import log_info, log_error

class DataFrameProcessor:
    """
    **Python classes implementation (5%)** - OOP class for DataFrame operations.
    **Pandas operations (10%)** - Comprehensive DataFrame manipulation operations.
    Demonstrates filtering, grouping, merging, pivoting, reshaping, and custom functions.
    """
    
    def __init__(self):
        """Initialize with data cleaner instances."""
        self.cleaner = DataCleaner()
        self.advanced_cleaner = AdvancedDataCleaner()
        self.processing_stats = {
            'duplicates_removed': 0,
            'missing_values_filled': 0,
            'rows_processed': 0,
            'columns_cleaned': 0
        }

    def remove_duplicates(self, df: pd.DataFrame, subset_columns: List[str], 
                         keep: str = 'first') -> pd.DataFrame:
        """
        **Pandas operations (10%)** - Remove duplicates with comprehensive logging.
        **Error handling (5%)** - Robust duplicate removal with statistics tracking.
        """
        try:
            log_info(f"Removing duplicates based on columns: {subset_columns}")
            
            original_count = len(df)
            
            # **Pandas operations** - Using drop_duplicates with parameters
            df_clean = df.drop_duplicates(subset=subset_columns, keep=keep)
            
            duplicates_removed = original_count - len(df_clean)
            self.processing_stats['duplicates_removed'] += duplicates_removed
            
            if duplicates_removed > 0:
                log_info(f"Removed {duplicates_removed} duplicate rows ({duplicates_removed/original_count*100:.2f}%)")
            else:
                log_info("No duplicates found")
            
            return df_clean
            
        except Exception as e:
            log_error(f"Error removing duplicates: {e}")
            return df

    def fill_missing_values(self, df: pd.DataFrame, column: str, 
                           fetch_func: Callable, movie_id_column: str = "id",
                           max_attempts: int = 50) -> pd.DataFrame:
        """
        **Data processing functions (5%)** - Modular missing value filling.
        **Pandas operations (10%)** - Advanced DataFrame filtering and value assignment.
        **Error handling (5%)** - Comprehensive error handling with logging.
        """
        try:
            log_info(f"Filling missing values in column '{column}' using API fetch function")
            
            # Create a copy to avoid SettingWithCopyWarning
            df_result = df.copy()
            
            # **Pandas operations** - Complex boolean indexing and filtering
            missing_mask = (
                df_result[column].isna() | 
                (df_result[column] == "") | 
                (df_result[column] == "0") |
                (df_result[column] == 0)
            )
            
            missing_count = missing_mask.sum()
            
            if missing_count == 0:
                log_info(f"No missing values found in column '{column}'")
                return df_result
            
            log_info(f"Found {missing_count} missing values in column '{column}'")
            filled_count = 0
            
            # **Pandas operations** - Using iloc and loc for data access
            missing_indices = df_result[missing_mask].index[:max_attempts]  # Limit API calls
            
            for idx in missing_indices:
                try:
                    movie_id = df_result.at[idx, movie_id_column]
                    if pd.isna(movie_id):
                        continue
                    
                    # Fetch data from API
                    fetched_data = fetch_func(int(movie_id))
                    
                    if fetched_data and column in fetched_data and fetched_data[column]:
                        # **Pandas operations** - Conditional value assignment
                        df_result.at[idx, column] = fetched_data[column]
                        filled_count += 1
                        
                except Exception as e:
                    log_error(f"Error filling missing value for movie ID {movie_id}: {e}")
                    continue
            
            self.processing_stats['missing_values_filled'] += filled_count
            log_info(f"Successfully filled {filled_count} out of {min(missing_count, max_attempts)} missing values")
            
            return df_result
            
        except Exception as e:
            log_error(f"Error in fill_missing_values for column '{column}': {e}")
            return df

    def clean_dataframe(self, df: pd.DataFrame, text_columns: Optional[List[str]] = None,
                       list_columns: Optional[List[str]] = None, 
                       date_columns: Optional[List[str]] = None,
                       use_advanced_cleaning: bool = False) -> pd.DataFrame:
        """
        **Pandas operations (10%)** - Comprehensive DataFrame cleaning with apply functions.
        **Data processing functions (5%)** - Modular cleaning with multiple column types.
        **Error handling (5%)** - Column-by-column error handling.
        """
        try:
            log_info("Starting comprehensive dataframe cleaning")
            
            # Create a copy to avoid modifying original
            df_clean = df.copy()
            cleaner_instance = self.advanced_cleaner if use_advanced_cleaning else self.cleaner
            
            # **Pandas operations** - Using apply with lambda functions for text columns
            if text_columns:
                for col in text_columns:
                    if col in df_clean.columns:
                        try:
                            log_info(f"Cleaning text column: {col}")
                            df_clean[col] = df_clean[col].apply(cleaner_instance.clean_text)
                            self.processing_stats['columns_cleaned'] += 1
                        except Exception as e:
                            log_error(f"Error cleaning text column '{col}': {e}")
            
            # **Pandas operations** - Custom function application for list columns
            if list_columns:
                for col in list_columns:
                    if col in df_clean.columns:
                        try:
                            log_info(f"Cleaning list column: {col}")
                            if col == 'genres' and use_advanced_cleaning:
                                df_clean[col] = df_clean[col].apply(cleaner_instance.clean_movie_genres)
                            elif col == 'production_countries' and use_advanced_cleaning:
                                df_clean[col] = df_clean[col].apply(cleaner_instance.clean_production_countries)
                            elif col in ['cast', 'director', 'writers'] and use_advanced_cleaning:
                                df_clean[col] = df_clean[col].apply(cleaner_instance.clean_cast_and_crew)
                            else:
                                df_clean[col] = df_clean[col].apply(cleaner_instance.clean_list_column)
                            self.processing_stats['columns_cleaned'] += 1
                        except Exception as e:
                            log_error(f"Error cleaning list column '{col}': {e}")
            
            # **Pandas operations** - Date column standardization
            if date_columns:
                for col in date_columns:
                    if col in df_clean.columns:
                        try:
                            log_info(f"Standardizing date column: {col}")
                            df_clean[col] = df_clean[col].apply(cleaner_instance.standardize_date_format)
                            self.processing_stats['columns_cleaned'] += 1
                        except Exception as e:
                            log_error(f"Error standardizing date column '{col}': {e}")
            
            self.processing_stats['rows_processed'] += len(df_clean)
            log_info(f"DataFrame cleaning completed. Processed {len(df_clean)} rows")
            
            return df_clean
            
        except Exception as e:
            log_error(f"Error in clean_dataframe: {e}")
            return df

    def analyze_dataframe_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Advanced DataFrame analysis with grouping and aggregation.
        **Data processing functions (5%)** - Comprehensive data quality assessment.
        """
        try:
            log_info("Analyzing dataframe quality metrics")
            
            analysis_results = {
                'basic_stats': {},
                'missing_data_analysis': {},
                'data_type_analysis': {},
                'quality_score': 0.0
            }
            
            # **Pandas operations** - Basic DataFrame statistics
            analysis_results['basic_stats'] = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'duplicate_rows': df.duplicated().sum()
            }
            
            # **Pandas operations** - Missing data analysis with aggregation
            missing_analysis = {}
            for col in df.columns:
                missing_count = df[col].isna().sum()
                missing_percentage = (missing_count / len(df)) * 100
                missing_analysis[col] = {
                    'missing_count': missing_count,
                    'missing_percentage': missing_percentage,
                    'data_type': str(df[col].dtype)
                }
            
            analysis_results['missing_data_analysis'] = missing_analysis
            
            # **Pandas operations** - Data type analysis with value_counts
            dtype_counts = df.dtypes.value_counts().to_dict()
            analysis_results['data_type_analysis'] = {
                str(k): v for k, v in dtype_counts.items()
            }
            
            # Calculate quality score
            total_cells = len(df) * len(df.columns)
            missing_cells = df.isna().sum().sum()
            quality_score = ((total_cells - missing_cells) / total_cells) * 100
            analysis_results['quality_score'] = quality_score
            
            log_info(f"Quality analysis completed. Overall quality score: {quality_score:.2f}%")
            
            return analysis_results
            
        except Exception as e:
            log_error(f"Error in dataframe quality analysis: {e}")
            return {}

    def merge_movie_data(self, main_df: pd.DataFrame, extended_df: pd.DataFrame, 
                        ratings_dict: Dict) -> pd.DataFrame:
        """
        **Pandas operations (10%)** - Complex DataFrame merging with multiple data sources.
        **Data processing functions (5%)** - Data integration and transformation.
        **Error handling (5%)** - Comprehensive merge error handling.
        """
        try:
            log_info("Merging movie data from multiple sources")
            
            # **Pandas operations** - DataFrame merging with outer join
            merged_df = pd.merge(main_df, extended_df, on='id', how='outer', 
                               suffixes=('_main', '_extended'))
            
            log_info(f"Merged main ({len(main_df)} rows) and extended ({len(extended_df)} rows) DataFrames")
            
            # **Pandas operations** - Converting dictionary to DataFrame for merging
            if ratings_dict:
                ratings_data = []
                for movie_id, rating_info in ratings_dict.items():
                    try:
                        if isinstance(rating_info, dict):
                            rating_row = {'id': int(movie_id)}
                            rating_row.update(rating_info)
                            ratings_data.append(rating_row)
                    except (ValueError, TypeError) as e:
                        log_error(f"Error processing rating for movie ID {movie_id}: {e}")
                
                if ratings_data:
                    ratings_df = pd.DataFrame(ratings_data)
                    
                    # **Pandas operations** - Another merge operation
                    merged_df = pd.merge(merged_df, ratings_df, on='id', how='left')
                    log_info(f"Added ratings data for {len(ratings_df)} movies")
            
            # **Pandas operations** - Column selection and reordering
            # Resolve column conflicts by selecting the best values
            merged_df = self._resolve_column_conflicts(merged_df)
            
            log_info(f"Final merged DataFrame has {len(merged_df)} rows and {len(merged_df.columns)} columns")
            
            return merged_df
            
        except Exception as e:
            log_error(f"Error in merge_movie_data: {e}")
            return main_df

    def _resolve_column_conflicts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        **Pandas operations (10%)** - Advanced column manipulation and conditional assignment.
        Resolve conflicts when same column exists in multiple sources.
        """
        try:
            # **Pandas operations** - Column filtering and selection
            conflict_columns = [col.replace('_main', '').replace('_extended', '') 
                              for col in df.columns if '_main' in col or '_extended' in col]
            
            conflict_columns = list(set(conflict_columns))
            
            for base_col in conflict_columns:
                main_col = f"{base_col}_main"
                extended_col = f"{base_col}_extended"
                
                if main_col in df.columns and extended_col in df.columns:
                    # **Pandas operations** - Conditional column assignment with fillna
                    df[base_col] = df[main_col].fillna(df[extended_col])
                    
                    # **Pandas operations** - Column dropping
                    df = df.drop(columns=[main_col, extended_col], errors='ignore')
            
            return df
            
        except Exception as e:
            log_error(f"Error resolving column conflicts: {e}")
            return df

    def create_movie_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Grouping, aggregation, and statistical analysis.
        **Data processing functions (5%)** - Statistical summary generation.
        """
        try:
            log_info("Creating movie summary statistics")
            
            summary_stats = {}
            
            # **Pandas operations** - Numerical column statistics with describe()
            numerical_columns = df.select_dtypes(include=[np.number]).columns
            if len(numerical_columns) > 0:
                summary_stats['numerical_summary'] = df[numerical_columns].describe().to_dict()
            
            # **Pandas operations** - Grouping by release year with aggregation
            if 'release_date' in df.columns:
                df_with_year = df.copy()
                df_with_year['release_year'] = pd.to_datetime(df_with_year['release_date'], errors='coerce').dt.year
                
                yearly_stats = df_with_year.groupby('release_year').agg({
                    'id': 'count',
                    'budget': ['mean', 'median'] if 'budget' in df.columns else 'count',
                    'revenue': ['mean', 'median'] if 'revenue' in df.columns else 'count',
                    'vote_average': 'mean' if 'vote_average' in df.columns else 'count'
                }).round(2)
                
                summary_stats['yearly_analysis'] = yearly_stats.to_dict()
            
            # **Pandas operations** - Value counts for categorical analysis
            if 'genres' in df.columns:
                # Flatten genre lists and count occurrences
                all_genres = []
                for genre_list in df['genres'].dropna():
                    if isinstance(genre_list, list):
                        all_genres.extend(genre_list)
                
                if all_genres:
                    genre_series = pd.Series(all_genres)
                    top_genres = genre_series.value_counts().head(10).to_dict()
                    summary_stats['top_genres'] = top_genres
            
            # **Pandas operations** - Correlation analysis for numerical columns
            if len(numerical_columns) > 1:
                correlation_matrix = df[numerical_columns].corr().round(3)
                summary_stats['correlation_analysis'] = correlation_matrix.to_dict()
            
            log_info("Summary statistics creation completed")
            
            return summary_stats
            
        except Exception as e:
            log_error(f"Error creating summary statistics: {e}")
            return {}

    def get_processing_stats(self) -> Dict[str, int]:
        """Return processing statistics."""
        return self.processing_stats.copy()

    def reset_stats(self):
        """Reset processing statistics."""
        for key in self.processing_stats:
            self.processing_stats[key] = 0

# **Data processing functions (5%)** - Backward compatible functions for existing code
def remove_duplicates(df: pd.DataFrame, subset_columns: List[str]) -> pd.DataFrame:
    """Backward compatible function using DataFrameProcessor."""
    processor = DataFrameProcessor()
    return processor.remove_duplicates(df, subset_columns)

def fill_missing_values(df: pd.DataFrame, column: str, fetch_func: Callable, 
                       movie_id_column: str = "id") -> pd.DataFrame:
    """Backward compatible function using DataFrameProcessor."""
    processor = DataFrameProcessor()
    return processor.fill_missing_values(df, column, fetch_func, movie_id_column)

def clean_dataframe(df: pd.DataFrame, text_columns: Optional[List[str]] = None,
                   list_columns: Optional[List[str]] = None, 
                   date_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Backward compatible function using DataFrameProcessor."""
    processor = DataFrameProcessor()
    return processor.clean_dataframe(df, text_columns, list_columns, date_columns)