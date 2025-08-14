from processors.file_handler import read_csv, write_csv, read_json, write_json
from processors.data_cleaning import clean_text, clean_list_column
from processors.tmdb_fetcher import fetch_movie_details
from processors.dataframe_ops import remove_duplicates, fill_missing
from utils.logger import log_info

def main():
    # Read files
    movies_main = read_csv("movies_main.csv")
    movie_extended = read_csv("movie_extended.csv")
    ratings = read_json("ratings.json")

    log_info("Files loaded successfully.")

    # Clean titles
    movies_main["title"] = movies_main["title"].apply(clean_text)

    # Clean list columns
    for col in ["genres", "production_companies", "production_countries", "spoken_languages"]:
        if col in movie_extended.columns:
            movie_extended[col] = movie_extended[col].apply(clean_list_column)

    # Remove duplicates
    movies_main = remove_duplicates(movies_main, ["id"])
    movie_extended = remove_duplicates(movie_extended, ["id"])

    # Fill missing from TMDb
    movies_main = fill_missing(movies_main, "budget", lambda mid: fetch_movie_details(mid).get("budget"))
    movies_main = fill_missing(movies_main, "revenue", lambda mid: fetch_movie_details(mid).get("revenue"))

    # Save cleaned files
    write_csv(movies_main, "movies_main_clean.csv")
    write_csv(movie_extended, "movie_extended_clean.csv")
    write_json(ratings, "ratings_clean.json")

    log_info("Cleaning process completed.")

if __name__ == "__main__":
    main()
