import requests
import os
from config import TMDB_API_KEY, debug_print

def get_last_page_retrieved():
    """
    Tracks which page of TMDb's 'popular' movies endpoint we fetched last time.
    If no record is found, we start with page=1.
    """
    file_name = "last_tmdb_page.txt"
    if not os.path.exists(file_name):
        return 0  # indicates no pages fetched yet
    with open(file_name, "r") as f:
        return int(f.read().strip())


def set_last_page_retrieved(page_num):
    """
    Store the current page number locally so next run will pick up from there.
    """
    file_name = "last_tmdb_page.txt"
    with open(file_name, "w") as f:
        f.write(str(page_num))


def fetch_tmdb_popular_movies(page):
    """
    Fetches 'popular' movies from TMDb for a given page number.
    Returns a list of movie dictionaries from the JSON response.
    """
    url = "https://api.themoviedb.org/3/movie/popular"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
        "page": page
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        debug_print(f"Error fetching TMDb data (status code {response.status_code})")
        return []

    data = response.json()
    # 'results' holds the list of movies
    return data.get("results", [])


def get_tmdb_movie_details(tmdb_id):
    """
    Given a TMDb movie ID, fetch the Movie Details endpoint to get:
       - imdb_id
       - budget
    Returns (imdb_id, budget).
    If request fails or data is missing, returns (None, 0).
    """
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    response = requests.get(url, params=params)

    if response.status_code != 200:
        debug_print(f"Error fetching TMDb details for movie_id={tmdb_id} (status {response.status_code})")
        return None, 0

    data = response.json()
    imdb_id = data.get("imdb_id", None)
    budget = data.get("budget", 0)
    return imdb_id, budget
