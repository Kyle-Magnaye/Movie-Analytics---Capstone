TMDB_API_KEY = "YOUR_TMDB_API_KEY"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Rate limiting settings
REQUEST_DELAY = 0.25  # 4 requests per second (TMDb limit is typically 40/10s)
MAX_RETRIES = 3