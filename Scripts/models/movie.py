from datetime import datetime
from typing import Optional, List

class Movie:
    """
    Movie class that combines data from both CSV files.
    Used for creating unified movie objects and validation.
    """
    def __init__(self, movie_id, title=None, release_date=None, budget=None, revenue=None, 
                 genres=None, production_companies=None, production_countries=None, 
                 spoken_languages=None):
        self.id = movie_id
        self.title = self._clean_title(title)
        self.release_date = self._parse_date(release_date)
        self.budget = self._parse_numeric(budget)
        self.revenue = self._parse_numeric(revenue)
        self.genres = genres or []
        self.production_companies = production_companies or []
        self.production_countries = production_countries or []
        self.spoken_languages = spoken_languages or []

    def _clean_title(self, title):
        if not title or str(title).strip() == '' or str(title).lower() == 'nan':
            return None
        return str(title).strip()

    def _parse_numeric(self, value):
        if value is None or str(value).strip() == '' or str(value).lower() == 'nan':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str):
        if not date_str or str(date_str).strip() == '' or str(date_str).lower() == 'nan':
            return None
        
        date_str = str(date_str).strip()
        
        # Try different date formats
        formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%Y"]
        
        for fmt in formats:
            try:
                if fmt == "%Y" and len(date_str) == 4:
                    return datetime.strptime(date_str, fmt).date()
                elif fmt != "%Y":
                    return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None

    @classmethod
    def from_dataframes(cls, main_row, extended_row):
        """Create Movie object from both CSV data sources"""
        return cls(
            movie_id=main_row.get('id'),
            title=main_row.get('title'),
            release_date=main_row.get('release_date'),
            budget=main_row.get('budget'),
            revenue=main_row.get('revenue'),
            genres=extended_row.get('genres', []) if extended_row is not None else [],
            production_companies=extended_row.get('production_companies', []) if extended_row is not None else [],
            production_countries=extended_row.get('production_countries', []) if extended_row is not None else [],
            spoken_languages=extended_row.get('spoken_languages', []) if extended_row is not None else []
        )
