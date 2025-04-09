"""
API Data Integration & Collection
---------------------------------
This script fetches movie data from two sources:
  1) OMDb (Open Movie Database) API
  2) TMDb (The Movie Database) API

It then stores this data into an SQLite database with the following tables:
  - Movies:       Basic info about the movie (title, year, IMDb ID, TMDb ID)
  - OMDbData:     Additional data from OMDb (genre, IMDb rating, etc.)
  - TMDbData:     Additional data from TMDb (popularity, vote count, etc.)

Incremental Fetch:
    - Each run of this program fetches a maximum of 25 new movies from TMDb
      and inserts them if they do not already exist in the database.
    - For each new movie from TMDb, the script attempts to fetch corresponding
      OMDb data (by IMDb ID) and store it as well.
    - By running the script multiple times, you can collect >=100 rows in total.

Usage:
    1. Install the 'requests' library if not already: pip install requests
    2. Update or set your API keys for OMDb and TMDb below (or as environment variables).
    3. Run this file multiple times until the desired amount of data is collected.
"""

import os
import sqlite3
import requests
import time
import matplotlib.pyplot as plt
import numpy as np

# --------------------------------------------------------------------
# API KEYS (Replace with your actual keys or pull from environment)
# --------------------------------------------------------------------
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "1ad61f09")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "ab67f773b96f6a5c2a52209b77fdd8b5")

# --------------------------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------------------------
DB_NAME = "movies.db"


def create_database():
    """
    Initializes the SQLite database and creates the necessary tables
    if they do not exist.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Create a table for storing movies (common fields).
    # We'll use a unique constraint on imdb_id or tmdb_id to avoid duplicates.
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

    # Create a table for storing data from OMDb
    # We'll link it back to Movies using movie_id as a foreign key.
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

    # Create a table for storing data from TMDb
    # We'll link it back to Movies using movie_id as a foreign key.
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

    conn.commit()
    conn.close()


def get_last_page_retrieved():
    """
    Tracks which page of TMDb's 'popular' movies endpoint we fetched last time.
    If no record is found, we start with page=1.

    You can store this in the database or in a local file.
    Here, we'll just store it in a simple local text file for simplicity.
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
    # 'results' is the key that holds the list of movies
    return data.get("results", [])


def insert_movie_if_not_exists(title, release_year, imdb_id, tmdb_id):
    """
    Inserts a movie into the Movies table if it doesn't already exist.
    Returns the 'id' of the movie (primary key in the Movies table).
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
        # The movie already exists, return its id
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


def insert_tmdb_data(movie_id, popularity, vote_count, average_vote):
    """
    Inserts data into the TMDbData table for the given movie_id.
    If there's existing TMDbData for that movie_id, update it instead.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if there's already TMDb data for this movie
    cur.execute(
        """
        SELECT id FROM TMDbData
        WHERE movie_id = ?
        """,
        (movie_id,)
    )
    row = cur.fetchone()

    if row:
        # Update existing record
        cur.execute(
            """
            UPDATE TMDbData
            SET popularity = ?, vote_count = ?, average_vote = ?
            WHERE movie_id = ?
            """,
            (popularity, vote_count, average_vote, movie_id)
        )
    else:
        # Insert new record
        cur.execute(
            """
            INSERT INTO TMDbData (movie_id, popularity, vote_count, average_vote)
            VALUES (?, ?, ?, ?)
            """,
            (movie_id, popularity, vote_count, average_vote)
        )

    conn.commit()
    conn.close()


def fetch_omdb_data(imdb_id):
    """
    Fetch detailed data for a movie from the OMDb API using the movie's IMDb ID.
    Returns a dictionary with relevant data (genre, imdbRating, imdbVotes).
    If the request fails or data is missing, returns None.
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
    Inserts data into the OMDbData table for the given movie_id.
    If there's existing OMDb data for that movie_id, update it instead.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if there's already OMDb data for this movie
    cur.execute(
        """
        SELECT id FROM OMDbData
        WHERE movie_id = ?
        """,
        (movie_id,)
    )
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


def get_imdb_id_from_tmdb(tmdb_id):
    """
    Given a TMDb movie ID, fetch more detailed info to get the IMDb ID.
    This is necessary because the 'popular' endpoint doesn't always include 'imdb_id'.
    """
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return None
    data = response.json()
    return data.get("imdb_id", None)


def show_data():
    """
    Prints out the contents of the Movies, OMDbData, and TMDbData tables
    to the console for verification.
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

    conn.close()


def main():
    """
    Main entry point:
    - Creates database/tables if they don't exist
    - Retrieves the last page fetched from TMDb
    - Fetches up to 25 new 'popular' movies
    - For each movie, inserts movie details into the Movies table
    - Fetches additional OMDb info (if IMDb ID is available) and inserts into OMDbData
    - Fetches additional TMDb info (popularity, vote counts) and inserts into TMDbData
    """
    create_database()

    last_page = get_last_page_retrieved()
    next_page = last_page + 1

    print(f"Fetching up to 25 new popular movies from TMDb (page {next_page}) ...")
    movies = fetch_tmdb_popular_movies(page=next_page)

    if not movies:
        print("No movies returned from TMDb. Try again later or check your API key.")
        return

    # Limit to 25 items max from this page (usually a page might have 20 results, but we ensure the spec)
    movies_to_process = movies[:25]

    for movie in movies_to_process:
        tmdb_id = movie.get("id")
        title = movie.get("title")
        release_year = None
        release_date = movie.get("release_date")
        if release_date and len(release_date) >= 4:
            release_year = int(release_date.split("-")[0])

        imdb_id = movie.get("imdb_id", None)  # TMDb doesn't always provide IMDb ID directly in 'popular' data
        # We'll need to fetch more details from TMDb's movie detail endpoint to get the IMDb ID
        imdb_id = get_imdb_id_from_tmdb(tmdb_id)  # see helper function above

        # Insert basic movie info into Movies table
        movie_id = insert_movie_if_not_exists(
            title=title,
            release_year=release_year,
            imdb_id=imdb_id,
            tmdb_id=tmdb_id
        )

        # Insert TMDb data (popularity, vote_count, average_vote)
        popularity = movie.get("popularity", 0)
        vote_count = movie.get("vote_count", 0)
        average_vote = movie.get("vote_average", 0)
        insert_tmdb_data(movie_id, popularity, vote_count, average_vote)

        # If we have an IMDb ID, fetch OMDb data
        if imdb_id:
            omdb_info = fetch_omdb_data(imdb_id)
            if omdb_info is not None:
                genre = omdb_info["Genre"]
                imdb_rating_str = omdb_info["imdbRating"]
                imdb_votes_str = omdb_info["imdbVotes"]

                # Convert rating and votes to numeric form if possible
                try:
                    imdb_rating = float(imdb_rating_str) if imdb_rating_str != "N/A" else None
                except:
                    imdb_rating = None
                try:
                    # Remove commas from votes if present
                    imdb_votes = int(imdb_votes_str.replace(",", "")) if imdb_votes_str != "N/A" else None
                except:
                    imdb_votes = None

                insert_omdb_data(movie_id, genre, imdb_rating, imdb_votes)

        # Sleep a bit to be polite to the API providers (optional)
        time.sleep(0.3)

    # Once done processing up to 25 items, mark this page as retrieved
    set_last_page_retrieved(next_page)
    print(f"Successfully processed page {next_page}. Run again to fetch more data.")

    # Print out the database contents (Movies, OMDbData, TMDbData) after each run
    show_data()


if __name__ == "__main__":
    main()
