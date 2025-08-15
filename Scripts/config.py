TMDB_API_KEY = "54e8dccdf76dabda6e23e45e60036ca6"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Rate limiting settings
REQUEST_DELAY = 0.021  # 4 requests per second (TMDb limit is typically 40/10s)
MAX_RETRIES = 3