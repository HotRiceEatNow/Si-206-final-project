import requests
from config import OMDB_API_KEY

def fetch_omdb_data(imdb_id):
    """
    Fetch detailed data for a movie from the OMDb API using the movie's IMDb ID.
    Returns a dict with (genre, imdbRating, imdbVotes) or None if not found.
    """
    if not imdb_id or imdb_id == "N/A":
        return None

    url = "http://www.omdbapi.com/"
    params = {
        "apikey": OMDB_API_KEY,
        "i": imdb_id,
        "plot": "short"
    }
    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Error fetching OMDb data for IMDb ID {imdb_id}")
        return None

    data = response.json()
    if data.get("Response") == "False":
        print(f"OMDb could not find data for IMDb ID {imdb_id}")
        return None

    # Extract fields of interest
    result = {
        "Genre": data.get("Genre", "N/A"),
        "imdbRating": data.get("imdbRating", "N/A"),
        "imdbVotes": data.get("imdbVotes", "N/A")
    }
    return result