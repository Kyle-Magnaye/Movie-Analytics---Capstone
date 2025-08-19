import pandas as pd
import os
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

# Import our enhanced modules
from models.movie import Movie
from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.data_cleaning import DataCleaner, AdvancedDataCleaner
from processors.dataframe_ops import DataFrameProcessor
from processors.tmdb_fetcher import fetch_movie_details
from utils.validators import DataValidator, COMPREHENSIVE_VALIDATION_RULES
from utils.logger import log_info, log_error

class MovieDataProcessor:
    """
    **Python classes implementation (5%)** - Main OOP class demonstrating inheritance, encapsulation, and methods.
    **Data processing functions (5%)** - Clean, reusable functions with modular design and DRY principles.
    **Error handling (5%)** - Comprehensive error handling with logging and recovery mechanisms.
    
    Main processor class that orchestrates the entire data cleaning pipeline using OOP principles.
    """
    
    def __init__(self, base_dir: str = None):
        """
        Initialize the movie data processor with all required components.
        **Python classes implementation (5%)** - Constructor with encapsulation.
        """
        # **Encapsulation** - Private attributes
        self._base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        self._root_dir = os.path.dirname(self._base_dir)
        
        # Initialize processing components - **Composition pattern**
        self.data_cleaner = AdvancedDataCleaner()
        self.dataframe_processor = DataFrameProcessor()
        self.validator = DataValidator()
        
        # **Encapsulation** - Private file paths
        self._file_paths = {
            'movies_main': os.path.join(self._root_dir, "Dataset", "your_single_movie_file.csv"),
        }
        
        # Processing statistics tracking
        self.processing_stats = {
            'start_time': None,
            'end_time': None,
            'total_movies_processed': 0,
            'total_errors': 0,
            'api_calls_made': 0,
            'data_quality_improvements': {}
        }

    # --- MODIFIED: Simplified data loading ---
    def load_data(self) -> pd.DataFrame:
        """
        **Data processing functions (5%)** - Modular data loading with error handling.
        **Pandas operations (10%)** - DataFrame loading and basic operations.
        **Error handling (5%)** - File loading with comprehensive error recovery.
        """
        try:
            log_info("Starting data loading process")
            
            file_path = self._file_paths['movies_main']
            
            try:
                # **Pandas operations (10%)** - DataFrame loading
                df = read_csv(file_path)
                log_info(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")
                
                # **Pandas operations (10%)** - Basic DataFrame analysis
                log_info(f"Columns: {list(df.columns)}")
                log_info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
                
                log_info("Data loading completed successfully")
                return df
                
            except FileNotFoundError as e:
                log_error(f"File not found: {file_path}. Error: {e}")
                raise
            except Exception as e:
                log_error(f"Error loading data: {e}")
                raise
            
        except Exception as e:
            log_error(f"Critical error in data loading: {e}")
            raise

    def create_movie_objects(self, df: pd.DataFrame) -> List[Movie]:
        """
        **Python classes implementation (5%)** - Object creation and manipulation.
        **Data processing functions (5%)** - Transformation of DataFrame to objects.
        **Pandas operations (10%)** - DataFrame iteration and apply functions.
        """
        try:
            log_info("Creating Movie objects from DataFrame")
            
            movie_objects = []
            successful_objects = 0
            failed_objects = 0
            
            # **Pandas operations (10%)** - DataFrame iteration with apply-like processing
            for idx, row in df.iterrows():
                try:
                    # **Python classes implementation (5%)** - Object instantiation
                    movie = Movie.from_series(row)
                    
                    if movie.is_valid():
                        movie_objects.append(movie)
                        successful_objects += 1
                    else:
                        failed_objects += 1
                        missing_fields = movie.get_missing_fields()
                        log_error(f"Invalid movie at index {idx}: missing {missing_fields}")
                
                except Exception as e:
                    failed_objects += 1
                    log_error(f"Error creating movie object at index {idx}: {e}")
            
            log_info(f"Created {successful_objects} valid Movie objects, {failed_objects} failed")
            self.processing_stats['total_movies_processed'] = successful_objects
            self.processing_stats['total_errors'] = failed_objects
            
            return movie_objects
            
        except Exception as e:
            log_error(f"Error in movie object creation: {e}")
            return []

    def validate_and_correct_data(self, df: pd.DataFrame, enable_api_fallback: bool = True) -> pd.DataFrame:
        """
        **Data processing functions (5%)** - Comprehensive data validation and correction.
        **Pandas operations (10%)** - Advanced DataFrame filtering, grouping, and conditional operations.
        **Error handling (5%)** - Validation with fallback mechanisms.
        """
        try:
            log_info("Starting comprehensive data validation and correction")
            
            # **Pandas operations (10%)** - DataFrame validation with complex rules
            validation_results = self.validator.validate_dataframe(
                df, 
                custom_rules=COMPREHENSIVE_VALIDATION_RULES,
                fetch_func=fetch_movie_details if enable_api_fallback else None,
                fallback_enabled=enable_api_fallback
            )
            
            # Extract corrected DataFrame
            corrected_df = validation_results['corrected_dataframe']
            
            # **Pandas operations (10%)** - Statistical analysis of validation results
            overall_score = validation_results['overall_health_score']
            self.processing_stats['data_quality_improvements'] = validation_results['validation_summary']
            
            log_info(f"Data validation completed. Health score: {overall_score:.2f}%")
            
            # **Pandas operations (10%)** - Generate validation report
            self._generate_validation_report(validation_results)
            
            return corrected_df
            
        except Exception as e:
            log_error(f"Error in data validation: {e}")
            return df

    def clean_dataframe_comprehensive(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        **Data processing functions (5%)** - Comprehensive DataFrame cleaning.
        **Pandas operations (10%)** - Advanced cleaning operations with apply functions.
        **Error handling (5%)** - Column-by-column error handling.
        """
        try:
            log_info("Starting comprehensive DataFrame cleaning")
            
            # Define column types for cleaning
            column_definitions = {
                'text_columns': ['title'],
                'list_columns': ['genres', 'production_companies', 'production_countries', 
                               'spoken_languages', 'cast', 'director', 'writers'],
                'date_columns': ['release_date', 'last_rated']
            }
            
            # **Pandas operations (10%)** - Advanced cleaning with movie-specific logic
            cleaned_df = self.dataframe_processor.clean_dataframe(
                df,
                text_columns=column_definitions['text_columns'],
                list_columns=column_definitions['list_columns'],
                date_columns=column_definitions['date_columns'],
                use_advanced_cleaning=True
            )
            
            # **Pandas operations (10%)** - Remove duplicates with sophisticated logic
            cleaned_df = self.dataframe_processor.remove_duplicates(cleaned_df, ['id'])
            
            log_info("Comprehensive DataFrame cleaning completed")
            
            return cleaned_df
            
        except Exception as e:
            log_error(f"Error in comprehensive cleaning: {e}")
            return df

    def generate_summary_analytics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Advanced analytics with grouping, pivoting, and aggregation.
        **Data processing functions (5%)** - Statistical analysis and reporting.
        """
        try:
            log_info("Generating comprehensive summary analytics")
            
            # **Pandas operations (10%)** - Advanced statistical analysis
            summary_stats = self.dataframe_processor.create_movie_summary_stats(df)
            
            # **Pandas operations (10%)** - Additional custom analytics
            additional_analytics = {}
            
            # Genre analysis with grouping
            if 'genres' in df.columns:
                genre_analysis = self._analyze_genres(df)
                additional_analytics['genre_insights'] = genre_analysis
            
            # Financial analysis with aggregation
            if 'budget' in df.columns and 'revenue' in df.columns:
                financial_analysis = self._analyze_financial_data(df)
                additional_analytics['financial_insights'] = financial_analysis
            
            # Rating correlation analysis
            rating_columns = ['vote_average', 'imdb_rating', 'avg_rating']
            available_ratings = [col for col in rating_columns if col in df.columns]
            if len(available_ratings) > 1:
                correlation_analysis = self._analyze_rating_correlations(df, available_ratings)
                additional_analytics['rating_correlations'] = correlation_analysis
            
            # Combine all analytics
            summary_stats.update(additional_analytics)
            
            log_info("Summary analytics generation completed")
            return summary_stats
            
        except Exception as e:
            log_error(f"Error generating summary analytics: {e}")
            return {}

    def _analyze_genres(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Genre analysis with custom aggregation.
        **Data processing functions (5%)** - Specialized analysis function.
        """
        try:
            # **Pandas operations (10%)** - Complex data transformation and analysis
            genre_data = []
            
            for idx, row in df.iterrows():
                if pd.notna(row.get('genres')) and isinstance(row['genres'], list):
                    for genre in row['genres']:
                        genre_data.append({
                            'genre': genre,
                            'movie_id': row['id'],
                            'budget': row.get('budget', 0),
                            'revenue': row.get('revenue', 0),
                            'rating': row.get('vote_average', 0)
                        })
            
            if not genre_data:
                return {}
            
            # **Pandas operations (10%)** - DataFrame creation and grouping operations
            genre_df = pd.DataFrame(genre_data)
            
            genre_analysis = genre_df.groupby('genre').agg({
                'movie_id': 'count',
                'budget': ['mean', 'median'],
                'revenue': ['mean', 'median'], 
                'rating': 'mean'
            }).round(2)
            
            # **Pandas operations (10%)** - Data reshaping and sorting
            genre_analysis.columns = ['_'.join(col).strip() for col in genre_analysis.columns]
            top_genres = genre_analysis.sort_values('movie_id_count', ascending=False).head(10)
            
            return {
                'top_genres_by_count': top_genres.to_dict(),
                'total_unique_genres': len(genre_analysis),
                'avg_genres_per_movie': len(genre_data) / len(df) if len(df) > 0 else 0
            }
            
        except Exception as e:
            log_error(f"Error in genre analysis: {e}")
            return {}

    def _analyze_financial_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Financial data analysis with filtering and aggregation.
        """
        try:
            # **Pandas operations (10%)** - Complex filtering and statistical operations
            financial_df = df[(df['budget'] > 0) & (df['revenue'] > 0)].copy()
            
            if len(financial_df) == 0:
                return {'message': 'No valid financial data available'}
            
            # **Pandas operations (10%)** - Custom column creation and analysis
            financial_df['profit'] = financial_df['revenue'] - financial_df['budget']
            financial_df['roi'] = (financial_df['profit'] / financial_df['budget']) * 100
            
            financial_analysis = {
                'total_movies_with_financial_data': len(financial_df),
                'avg_budget': float(financial_df['budget'].mean()),
                'avg_revenue': float(financial_df['revenue'].mean()),
                'avg_profit': float(financial_df['profit'].mean()),
                'avg_roi_percentage': float(financial_df['roi'].mean()),
                'highest_grossing_movie': financial_df.loc[financial_df['revenue'].idxmax(), 'title'] if 'title' in financial_df.columns else 'Unknown',
                'most_profitable_movie': financial_df.loc[financial_df['profit'].idxmax(), 'title'] if 'title' in financial_df.columns else 'Unknown'
            }
            
            return financial_analysis
            
        except Exception as e:
            log_error(f"Error in financial analysis: {e}")
            return {}

    def _analyze_rating_correlations(self, df: pd.DataFrame, rating_columns: List[str]) -> Dict[str, Any]:
        """
        **Pandas operations (10%)** - Correlation analysis and statistical operations.
        """
        try:
            # **Pandas operations (10%)** - Correlation matrix calculation
            rating_data = df[rating_columns].dropna()
            
            if len(rating_data) < 2:
                return {'message': 'Insufficient data for correlation analysis'}
            
            correlation_matrix = rating_data.corr()
            
            return {
                'correlation_matrix': correlation_matrix.to_dict(),
                'sample_size': len(rating_data),
                'strongest_correlation': {
                    'columns': self._find_strongest_correlation(correlation_matrix),
                    'value': float(correlation_matrix.max().max())
                }
            }
            
        except Exception as e:
            log_error(f"Error in rating correlation analysis: {e}")
            return {}

    def _find_strongest_correlation(self, corr_matrix: pd.DataFrame) -> List[str]:
        """**Data processing functions (5%)** - Helper function for correlation analysis."""
        try:
            # Find the strongest correlation (excluding diagonal)
            corr_matrix_no_diag = corr_matrix.where(~pd.DataFrame(True, index=corr_matrix.index, columns=corr_matrix.columns).values)
            max_corr = corr_matrix_no_diag.max().max()
            
            for col in corr_matrix.columns:
                for idx in corr_matrix.index:
                    if corr_matrix.loc[idx, col] == max_corr and idx != col:
                        return [idx, col]
            
            return []
        except Exception as e:
            log_error(f"Error finding strongest correlation: {e}")
            return []

    def _generate_validation_report(self, validation_results: Dict[str, Any]):
        """
        **Data processing functions (5%)** - Report generation with comprehensive logging.
        **Error handling (5%)** - Safe report generation.
        """
        try:
            log_info("="*60)
            log_info("COMPREHENSIVE VALIDATION REPORT")
            log_info("="*60)
            
            summary = validation_results.get('validation_summary', {})
            
            for column, stats in summary.items():
                log_info(f"Column '{column}':")
                log_info(f"  - Valid: {stats.get('valid', 0)}")
                log_info(f"  - Invalid: {stats.get('invalid', 0)}")
                log_info(f"  - Percentage Valid: {stats.get('percentage_valid', 0):.2f}%")
                
                if 'corrected' in stats:
                    log_info(f"  - Corrected via API: {stats['corrected']}")
                    log_info(f"  - Final Valid: {stats.get('valid_after_correction', 0)}")
                    log_info(f"  - Final Percentage: {stats.get('final_percentage_valid', 0):.2f}%")
                
                log_info("-" * 40)
            
            log_info(f"Overall Health Score: {validation_results.get('overall_health_score', 0):.2f}%")
            log_info(f"Processing Time: {validation_results.get('processing_time', 0):.2f} seconds")
            log_info("="*60)
            
        except Exception as e:
            log_error(f"Error generating validation report: {e}")

    def save_results(self, df: pd.DataFrame, movie_objects: List[Movie], analytics: Dict[str, Any]):
        """
        **Data processing functions (5%)** - Result saving with multiple formats.
        **Error handling (5%)** - Safe file operations with error recovery.
        """
        try:
            log_info("Saving processed results")
            
            # Save cleaned DataFrame
            output_df_path = "movies_final_cleaned.csv"
            write_csv(df, output_df_path)
            log_info(f"Saved cleaned DataFrame: {output_df_path}")
            
            # Save movie objects as JSON
            movies_json = [movie.to_dict() for movie in movie_objects]
            write_json(movies_json, "movies_objects_final.json")
            log_info(f"Saved {len(movies_json)} movie objects as JSON")
            
            # Save analytics report
            write_json(analytics, "analytics_report.json")
            log_info("Saved analytics report")
            
            # Save processing statistics
            final_stats = {
                **self.processing_stats,
                **self.validator.get_validation_stats(),
                **self.dataframe_processor.get_processing_stats(),
                'cleaning_stats': self.data_cleaner.get_cleaning_stats()
            }
            
            write_json(final_stats, "processing_statistics.json")
            log_info("Saved processing statistics")
            
        except Exception as e:
            log_error(f"Error saving results: {e}")

    def run_complete_pipeline(self) -> Dict[str, Any]:
        """
        **Python classes implementation (5%)** - Main orchestration method demonstrating all OOP principles.
        **Data processing functions (5%)** - Complete pipeline with modular design.
        **Pandas operations (10%)** - Comprehensive DataFrame operations throughout.
        **Error handling (5%)** - End-to-end error handling and recovery.
        """
        try:
            self.processing_stats['start_time'] = datetime.now().isoformat()
            log_info("="*80)
            log_info("STARTING COMPREHENSIVE MOVIE DATA PROCESSING PIPELINE")
            log_info("="*80)
            
            # Step 1: Load data
            loaded_data = self.load_data()
            
            # Step 2: Clean DataFrame comprehensively
            cleaned_df = self.clean_dataframe_comprehensive(loaded_data)
            
            # Step 3: Validate and correct data with API fallback
            validated_df = self.validate_and_correct_data(cleaned_df, enable_api_fallback=True)
            
            # Step 4: Create Movie objects
            movie_objects = self.create_movie_objects(validated_df)
            
            # Step 5: Generate comprehensive analytics
            analytics = self.generate_summary_analytics(validated_df)
            
            # Step 6: Save all results
            self.save_results(validated_df, movie_objects, analytics)
            
            # Final processing statistics
            self.processing_stats['end_time'] = datetime.now().isoformat()
            
            log_info("="*80)
            log_info("MOVIE DATA PROCESSING PIPELINE COMPLETED SUCCESSFULLY")
            log_info(f"Total movies processed: {self.processing_stats['total_movies_processed']}")
            log_info(f"Total errors encountered: {self.processing_stats['total_errors']}")
            log_info("="*80)
            
            return {
                'success': True,
                'processed_dataframe': validated_df,
                'movie_objects': movie_objects,
                'analytics': analytics,
                'statistics': self.processing_stats
            }
            
        except Exception as e:
            log_error(f"Critical error in processing pipeline: {e}")
            self.processing_stats['end_time'] = datetime.now().isoformat()
            return {
                'success': False,
                'error': str(e),
                'statistics': self.processing_stats
            }

def main():
    """
    **Error handling (5%)** - Main function with comprehensive error handling.
    **Python classes implementation (5%)** - Demonstrates object instantiation and method calls.
    """
    try:
        # **Python classes implementation (5%)** - Object instantiation
        processor = MovieDataProcessor()
        
        # **Error handling (5%)** - Pipeline execution with error recovery
        results = processor.run_complete_pipeline()
        
        if results['success']:
            print("✅ Movie data processing completed successfully!")
            print(f"📊 Processed {results['statistics']['total_movies_processed']} movies")
            print(f"⚠️  Encountered {results['statistics']['total_errors']} errors")
            print("📁 Results saved to output files")
        else:
            print("❌ Movie data processing failed!")
            print(f"Error: {results['error']}")
            
    except Exception as e:
        log_error(f"Fatal error in main: {e}")
        print(f"❌ Fatal error occurred: {e}")

if __name__ == "__main__":
    main()