import requests
import time
from config import TMDB_API_KEY, TMDB_BASE_URL, REQUEST_DELAY, MAX_RETRIES
from utils.logger import log_error, log_info

class TMDbFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - time_since_last)
        self.last_request_time = time.time()
    
    def fetch_movie_details(self, movie_id):
        """Fetch movie details with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                url = f"{TMDB_BASE_URL}/movie/{movie_id}"
                params = {"api_key": TMDB_API_KEY}
                
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                log_info(f"Successfully fetched data for movie ID: {movie_id}")
                return data
                
            except requests.exceptions.RequestException as e:
                log_error(f"Attempt {attempt + 1} failed for movie ID {movie_id}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    log_error(f"All attempts failed for movie ID {movie_id}")
                    return {}
            except Exception as e:
                log_error(f"Unexpected error for movie ID {movie_id}: {e}")
                return {}
        
        return {}

# Global instance
tmdb_fetcher = TMDbFetcher()

def fetch_movie_details(movie_id):
    """Wrapper function for backward compatibility"""
    return tmdb_fetcher.fetch_movie_details(movie_id)