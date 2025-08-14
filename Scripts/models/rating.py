class Rating:
    def __init__(self, movie_id, avg_rating=None, total_ratings=None, std_dev=None, last_rated=None):
        self.movie_id = movie_id
        self.avg_rating = float(avg_rating) if avg_rating is not None else None
        self.total_ratings = int(total_ratings) if total_ratings is not None else None
        self.std_dev = float(std_dev) if std_dev is not None else None
        self.last_rated = last_rated
