from datetime import datetime

class Movie:
    def __init__(self, movie_id, title, release_date, budget, revenue, genres=None,
                 production_companies=None, production_countries=None, spoken_languages=None):
        self.id = movie_id
        self.title = title.strip() if title else None
        self.release_date = self._parse_date(release_date)
        self.budget = float(budget) if budget else None
        self.revenue = float(revenue) if revenue else None
        self.genres = genres or []
        self.production_companies = production_companies or []
        self.production_countries = production_countries or []
        self.spoken_languages = spoken_languages or []

    def _parse_date(self, date_str):
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(date_str, "%d/%m/%Y").date()
            except ValueError:
                return None
