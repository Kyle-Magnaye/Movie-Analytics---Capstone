import requests
from config import TMDB_API_KEY, TMDB_BASE_URL
from utils.logger import log_error

def fetch_movie_details(movie_id):
    try:
        url = f"{TMDB_BASE_URL}/movie/{movie_id}"
        params = {"api_key": TMDB_API_KEY}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_error(f"TMDb fetch failed for {movie_id}: {e}")
        return {}
