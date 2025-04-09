"""
API Data Integration & Collection
---------------------------------
This script fetches movie data from three sources:
  1) OMDb (Open Movie Database) API
  2) TMDb (The Movie Database) API
  3) SerpApi (Showtimes from Google results)

It stores the data into an SQLite database with these tables:
  - Movies:       Basic info (title, year, IMDb ID, TMDb ID)
  - OMDbData:     Additional data from OMDb (genre, rating, votes)
  - TMDbData:     Additional data from TMDb (popularity, vote count, average_vote, budget)
  - ShowtimesData:Showtimes info from SerpApi (slots_count per day)

Incremental Fetch:
    - Each run fetches up to 25 new movies from TMDb's "popular" list
    - Inserts them if they do not already exist
    - Also fetches OMDb data by IMDb ID
    - Fetches showtimes data from SerpApi for each movie title
    - By running repeatedly, you can gather >=100 rows total.

Usage:
    1. Install 'requests' if not already: pip install requests
    2. Update or set your API keys for OMDb, TMDb, and SerpApi below (or as environment variables).
    3. Run multiple times until you have enough data.
"""

import os
import sqlite3
import requests
import time
from datetime import date

# --------------------------------------------------------------------
# API KEYS (Replace with your actual keys or set environment variables)
# --------------------------------------------------------------------
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "1ad61f09")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "ab67f773b96f6a5c2a52209b77fdd8b5")
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY", "ec44919024d8492b657c179a691b512169778a1e5d8c4f34ae2d2738db9d6415")

# --------------------------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------------------------
DB_NAME = "movies.db"


def create_database():
    """
    Initializes the SQLite database and creates/updates the necessary tables
    if they do not exist. Also checks if 'budget' column exists in TMDbData,
    adding it if it's missing, and ensures ShowtimesData exists.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # -- Movies table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            release_year INTEGER,
            imdb_id TEXT UNIQUE,
            tmdb_id INTEGER UNIQUE
        )
        """
    )

    # -- OMDbData table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS OMDbData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id INTEGER,
            genre TEXT,
            imdb_rating REAL,
            imdb_votes INTEGER,
            FOREIGN KEY(movie_id) REFERENCES Movies(id)
        )
        """
    )

    # -- TMDbData table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS TMDbData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id INTEGER,
            popularity REAL,
            vote_count INTEGER,
            average_vote REAL,
            FOREIGN KEY(movie_id) REFERENCES Movies(id)
        )
        """
    )

    # -- Check/add 'budget' column to TMDbData if missing
    cur.execute("PRAGMA table_info(TMDbData)")
    existing_columns = [row[1] for row in cur.fetchall()]  # row[1] is column name
    if "budget" not in existing_columns:
        cur.execute("ALTER TABLE TMDbData ADD COLUMN budget INTEGER")

    # -- ShowtimesData table for SerpApi showtimes
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ShowtimesData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id INTEGER,
            show_date TEXT,
            slots_count INTEGER,
            FOREIGN KEY(movie_id) REFERENCES Movies(id)
        )
        """
    )

    conn.commit()
    conn.close()


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


def fetch_tmdb_popular_movies(page=1):
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
        print(f"Error fetching TMDb data (status code {response.status_code})")
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
        print(f"Error fetching TMDb details for movie_id={tmdb_id} (status {response.status_code})")
        return None, 0

    data = response.json()
    imdb_id = data.get("imdb_id", None)
    budget = data.get("budget", 0)
    return imdb_id, budget


def insert_movie_if_not_exists(title, release_year, imdb_id, tmdb_id):
    """
    Inserts a movie into the Movies table if it doesn't already exist.
    Returns the 'id' (PK) of the movie in Movies.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if a movie with this imdb_id or tmdb_id already exists
    cur.execute(
        """
        SELECT id FROM Movies
        WHERE imdb_id = ? OR tmdb_id = ?
        """,
        (imdb_id, tmdb_id)
    )
    row = cur.fetchone()

    if row:
        # The movie already exists
        movie_id = row[0]
    else:
        # Insert the movie
        cur.execute(
            """
            INSERT INTO Movies (title, release_year, imdb_id, tmdb_id)
            VALUES (?, ?, ?, ?)
            """,
            (title, release_year, imdb_id, tmdb_id)
        )
        movie_id = cur.lastrowid
        conn.commit()

    conn.close()
    return movie_id


def insert_tmdb_data(movie_id, popularity, vote_count, average_vote, budget):
    """
    Inserts or updates data in the TMDbData table, including the 'budget' field.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if there's already TMDb data for this movie
    cur.execute("SELECT id FROM TMDbData WHERE movie_id = ?", (movie_id,))
    row = cur.fetchone()

    if row:
        # Update existing record
        cur.execute(
            """
            UPDATE TMDbData
            SET popularity = ?, vote_count = ?, average_vote = ?, budget = ?
            WHERE movie_id = ?
            """,
            (popularity, vote_count, average_vote, budget, movie_id)
        )
    else:
        # Insert new record
        cur.execute(
            """
            INSERT INTO TMDbData (movie_id, popularity, vote_count, average_vote, budget)
            VALUES (?, ?, ?, ?, ?)
            """,
            (movie_id, popularity, vote_count, average_vote, budget)
        )

    conn.commit()
    conn.close()


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


def insert_omdb_data(movie_id, genre, imdb_rating, imdb_votes):
    """
    Inserts or updates data in the OMDbData table for the given movie_id.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if there's already OMDb data for this movie
    cur.execute("SELECT id FROM OMDbData WHERE movie_id = ?", (movie_id,))
    row = cur.fetchone()

    if row:
        # Update existing record
        cur.execute(
            """
            UPDATE OMDbData
            SET genre = ?, imdb_rating = ?, imdb_votes = ?
            WHERE movie_id = ?
            """,
            (genre, imdb_rating, imdb_votes, movie_id)
        )
    else:
        # Insert new record
        cur.execute(
            """
            INSERT INTO OMDbData (movie_id, genre, imdb_rating, imdb_votes)
            VALUES (?, ?, ?, ?)
            """,
            (movie_id, genre, imdb_rating, imdb_votes)
        )

    conn.commit()
    conn.close()


# --------------------------------------------------------------------
# SERPAPI SHOWTIMES INTEGRATION
# --------------------------------------------------------------------
def fetch_showtime_slots(movie_title):
    """
    Calls SerpApi's Showtimes endpoint to scrape a movie's showtimes from Google.
    We sum up the number of showtimes (slots) across all theaters for 'today'.

    Returns an integer count of how many times the movie is playing
    (slots_count) or 0 if none found or if there's an error (HTTP 400, etc.).

    NOTE: SerpApi requires certain parameters for 'google_showtimes':
      - location, hl, gl, start_date, end_date, etc.
      - If the movie is not in theaters (older or future release), we might get 0 slots.
    """
    # For demonstration, we use today's date for both start_date and end_date.
    today_str = date.today().isoformat()  # e.g. '2025-04-09'

    # Construct the search query (could also do e.g. f"{movie_title} movie times near me")
    query_str = f"{movie_title} showtimes near me"

    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google_showtimes",
        "q": query_str,
        "hl": "en",
        "gl": "us",
        "location": "New York,NY,USA",
        "start_date": today_str,
        "end_date": today_str,
        "movie_times": "1",  # recommended for showtimes queries
        "api_key": SERPAPI_API_KEY
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching showtime data from SerpApi for '{movie_title}' (status {response.status_code})")
        return 0

    data = response.json()

    # 'showtimes' results are often nested. We'll attempt a simplified parse:
    showtimes_results = data.get("showtimes", [])
    total_slots = 0

    for theater_info in showtimes_results:
        for show_date_info in theater_info.get("showing", []):
            times_list = show_date_info.get("times", [])
            total_slots += len(times_list)

    return total_slots


def insert_showtimes_data(movie_id, slots_count):
    """
    Inserts or updates the daily showtimes count in the ShowtimesData table
    for the given movie_id. We'll store the date as 'today'.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    today_str = date.today().isoformat()

    # Check if we already inserted data for this movie/today
    cur.execute(
        "SELECT id FROM ShowtimesData WHERE movie_id = ? AND show_date = ?",
        (movie_id, today_str)
    )
    row = cur.fetchone()

    if row:
        # Update existing record for today's date
        cur.execute(
            """
            UPDATE ShowtimesData
            SET slots_count = ?
            WHERE movie_id = ? AND show_date = ?
            """,
            (slots_count, movie_id, today_str)
        )
    else:
        # Insert new record
        cur.execute(
            """
            INSERT INTO ShowtimesData (movie_id, show_date, slots_count)
            VALUES (?, ?, ?)
            """,
            (movie_id, today_str, slots_count)
        )

    conn.commit()
    conn.close()


def show_data():
    """
    Prints out the contents of all tables, including the ShowtimesData.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    print("\n=== Movies Table ===")
    cur.execute("SELECT * FROM Movies")
    movies_rows = cur.fetchall()
    for row in movies_rows:
        print(row)

    print("\n=== OMDbData Table ===")
    cur.execute("SELECT * FROM OMDbData")
    omdb_rows = cur.fetchall()
    for row in omdb_rows:
        print(row)

    print("\n=== TMDbData Table ===")
    cur.execute("SELECT * FROM TMDbData")
    tmdb_rows = cur.fetchall()
    for row in tmdb_rows:
        print(row)

    print("\n=== ShowtimesData Table ===")
    cur.execute("SELECT * FROM ShowtimesData")
    showtimes_rows = cur.fetchall()
    for row in showtimes_rows:
        print(row)

    conn.close()


def main():
    """
    Main function:
      - Creates/updates the database schema
      - Figures out which page of TMDb 'popular' movies we should fetch
      - Fetches up to 25 new movies from TMDb
      - For each movie:
         * calls TMDb details for imdb_id & budget
         * updates the Movies table
         * updates the TMDbData table (popularity, vote_count, avg_vote, budget)
         * calls OMDb if imdb_id is present to fill OMDbData
         * calls SerpApi to get the daily showtimes slots_count
      - Prints the database tables
    """
    create_database()

    last_page = get_last_page_retrieved()
    next_page = last_page + 1

    print(f"Fetching up to 25 new popular movies from TMDb (page {next_page}) ...")
    movies = fetch_tmdb_popular_movies(page=next_page)
    if not movies:
        print("No movies returned from TMDb. Try again later or check your API key.")
        return

    movies_to_process = movies[:25]  # limit to 25

    for movie in movies_to_process:
        tmdb_id = movie.get("id")
        title = movie.get("title")
        release_date = movie.get("release_date", "")
        release_year = None
        if release_date and len(release_date) >= 4:
            release_year = int(release_date.split("-")[0])

        popularity = movie.get("popularity", 0.0)
        vote_count = movie.get("vote_count", 0)
        average_vote = movie.get("vote_average", 0.0)

        # Use TMDb details for imdb_id & budget
        imdb_id, budget = get_tmdb_movie_details(tmdb_id)

        # Insert/update Movies
        movie_id = insert_movie_if_not_exists(
            title=title,
            release_year=release_year,
            imdb_id=imdb_id,
            tmdb_id=tmdb_id
        )

        # Insert/update TMDbData
        insert_tmdb_data(movie_id, popularity, vote_count, average_vote, budget)

        # If IMDb ID is available, fetch OMDb
        if imdb_id:
            omdb_info = fetch_omdb_data(imdb_id)
            if omdb_info is not None:
                genre = omdb_info["Genre"]
                imdb_rating_str = omdb_info["imdbRating"]
                imdb_votes_str = omdb_info["imdbVotes"]

                # Convert rating/votes if possible
                try:
                    imdb_rating = float(imdb_rating_str) if imdb_rating_str != "N/A" else None
                except:
                    imdb_rating = None
                try:
                    imdb_votes = int(imdb_votes_str.replace(",", "")) if imdb_votes_str != "N/A" else None
                except:
                    imdb_votes = None

                insert_omdb_data(movie_id, genre, imdb_rating, imdb_votes)

        # --- NEW: Fetch SerpApi showtimes (slots_count) for this movie title
        slots_count = fetch_showtime_slots(title)
        insert_showtimes_data(movie_id, slots_count)

        # Be kind to the APIs
        time.sleep(0.3)

    set_last_page_retrieved(next_page)
    print(f"Successfully processed page {next_page}. Run again to fetch more data.")

    # Finally, show the database tables
    show_data()


if __name__ == "__main__":
    main()
