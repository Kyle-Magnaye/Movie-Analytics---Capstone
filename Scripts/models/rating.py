class Rating:
    def __init__(self, movie_id, avg_rating=None, total_ratings=None, 
                 std_dev=None, last_rated=None):
        self.movie_id = movie_id
        self.avg_rating = self._parse_numeric(avg_rating)
        self.total_ratings = self._parse_integer(total_ratings)
        self.std_dev = self._parse_numeric(std_dev)
        self.last_rated = self._parse_date(last_rated)

    def _parse_numeric(self, value):
        if value is None or str(value).strip() == '' or str(value).lower() == 'nan':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_integer(self, value):
        if value is None or str(value).strip() == '' or str(value).lower() == 'nan':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str):
        if not date_str or str(date_str).strip() == '' or str(date_str).lower() == 'nan':
            return None
        
        date_str = str(date_str).strip()
        formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None