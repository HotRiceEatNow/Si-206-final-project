# File to gather data from the different various sources
# (Box Office Mojo via BeautifulSoup, TMDb API, OMDb API, SerpAPI)

import sqlite3
import os
import requests
import time

DB_NAME = "movies.db"

# TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "ab67f773b96f6a5c2a52209b77fdd8b5")
TMDB_API_KEY = "ab67f773b96f6a5c2a52209b77fdd8b5"
# OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "1ad61f09")
OMDB_API_KEY = "1ad61f09"


def create_database():
    """
    Initializes the SQLite database:
    - Genres table mapping unique ID to a Genre string
    - Movies table giving details about each specific movie
    - Showtimes table which is a one-to-many relationship
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # -- Genres table (basic identifiers)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
        """
    )

    # -- Movies table (combined TMDb and OMDb data)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            release_year INTEGER,
            genre_id INTEGER,
            tmdb_id INTEGER UNIQUE,
            imdb_id TEXT UNIQUE,
            popularity REAL,
            vote_count INTEGER,
            average_vote REAL,
            budget INTEGER,
            imdb_rating REAL,
            imdb_votes INTEGER,
            FOREIGN KEY(genre_id) REFERENCES Genres(id)
        )
        """
        # popularity, vote_count, average_vote, and budget are from TMDb API
        # imdb_rating, and imdb_votes are from OMDb API
    )

    # -- Showtimes table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Showtimes (
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


def get_or_create_genre_id(genre_name):
    """
    Returns the genre_id from the Genres table.
    If the genre does not exist, inserts it and returns the new ID.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Normalize genre_name (strip spaces, make consistent case if needed)
    genre_name = genre_name.strip()

    # Check if the genre already exists
    cur.execute("SELECT id FROM Genres WHERE name = ?", (genre_name,))
    row = cur.fetchone()

    if row:
        genre_id = row[0]
    else:
        # Insert the new genre
        cur.execute("INSERT INTO Genres (name) VALUES (?)", (genre_name,))
        genre_id = cur.lastrowid
        conn.commit()

    conn.close()
    return genre_id


def insert_or_update_movie(
    title,
    release_year,
    genre_id,
    tmdb_id,
    imdb_id,
    popularity,
    vote_count,
    average_vote,
    budget,
    imdb_rating,
    imdb_votes
):
    """
    Inserts a new movie or updates an existing one based on tmdb_id.
    Returns the movie_id.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check if movie with the given tmdb_id already exists
    cur.execute("SELECT id FROM Movies WHERE tmdb_id = ?", (tmdb_id,))
    row = cur.fetchone()

    if row:
        # Update existing record
        movie_id = row[0]
        cur.execute(
            """
            UPDATE Movies
            SET title = ?, release_year = ?, genre_id = ?, imdb_id = ?, popularity = ?,
                vote_count = ?, average_vote = ?, budget = ?, imdb_rating = ?, imdb_votes = ?
            WHERE id = ?
            """,
            (
                title,
                release_year,
                genre_id,
                imdb_id,
                popularity,
                vote_count,
                average_vote,
                budget,
                imdb_rating,
                imdb_votes,
                movie_id,
            ),
        )
    else:
        # Insert new record
        cur.execute(
            """
            INSERT INTO Movies (
                title, release_year, genre_id, tmdb_id, imdb_id, popularity,
                vote_count, average_vote, budget, imdb_rating, imdb_votes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                release_year,
                genre_id,
                tmdb_id,
                imdb_id,
                popularity,
                vote_count,
                average_vote,
                budget,
                imdb_rating,
                imdb_votes,
            ),
        )
        movie_id = cur.lastrowid

    conn.commit()
    conn.close()
    return movie_id


def print_database_state():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    print("\n-- Genres Table --")
    cur.execute("SELECT id, name FROM Genres ORDER BY id")
    genres = cur.fetchall()
    if genres:
        for g in genres:
            print(f"  {g[0]}: {g[1]}")
    else:
        print("  (empty)")

    print("\n-- Movies Table --")
    cur.execute("""
        SELECT id, title, release_year, genre_id, tmdb_id, imdb_id, popularity, vote_count,
            average_vote, budget, imdb_rating, imdb_votes
        FROM Movies
        ORDER BY id DESC
    """)
    movies = cur.fetchall()

    def fmt(val, fmt_str="{:,}"):
        return fmt_str.format(val) if val is not None else "None"

    if movies:
        for m in movies:
            print(f"""  {m[0]}: {m[1]} ({m[2]})
        Genre ID: {m[3]}
        TMDb ID: {m[4]} | IMDb ID: {m[5]}
        Popularity: {fmt(m[6], "{:.2f}")} | Vote Count: {fmt(m[7])} | Avg Vote: {fmt(m[8], "{:.1f}")}
        Budget: ${fmt(m[9])} | IMDb Rating: {fmt(m[10], "{:.1f}")} | IMDb Votes: {fmt(m[11])}
    """)
    else:
        print("  (empty)")

    conn.close()


def main():
    """
    The main function to gather data from various sources and populate the SQLite database.
    """
    # -- Create SQLite database and tables if not already created
    create_database()

    print("\n==== DATABASE STATE BEFORE RUN ====")
    print_database_state()

    last_page = get_last_page_retrieved()
    next_page = last_page + 1

    print(f"\nFetching up to 25 new popular movies from TMDb (page {next_page}) ...")
    movies = fetch_tmdb_popular_movies(page=next_page)
    if not movies:
        print("No movies returned from TMDb. Try again later or check your API key.")
        return

    # Limit to 25 movies
    movies_to_process = movies[:25]

    print("\n==== MOVIES TO PROCESS ====")
    for idx, movie in enumerate(movies_to_process, 1):
        print(f"{idx:>2}. {movie.get('title')} (TMDb ID: {movie.get('id')})")

    for movie in movies_to_process:
        tmdb_id = movie.get("id")
        title = movie.get("title")
        release_date = movie.get("release_date", "")
        release_year = int(release_date.split("-")[0]) if release_date and len(release_date) >= 4 else None
        popularity = movie.get("popularity", 0.0)
        vote_count = movie.get("vote_count", 0)
        average_vote = movie.get("vote_average", 0.0)

        print(f"\n---- PROCESSING MOVIE: {title} (TMDb ID: {tmdb_id}) ----")
        print(f"TMDb Data:")
        print(f"  • Release Year : {release_year}")
        print(f"  • Popularity   : {popularity}")
        print(f"  • Vote Count   : {vote_count}")
        print(f"  • Avg. Vote    : {average_vote}")

        # Fetch TMDb details
        imdb_id, budget = get_tmdb_movie_details(tmdb_id)
        print(f"  • IMDb ID      : {imdb_id}")
        print(f"  • Budget       : {budget}")

        genre_id = None
        imdb_rating = None
        imdb_votes = None
        genre_name = None

        if imdb_id:
            omdb_info = fetch_omdb_data(imdb_id)
            if omdb_info:
                genre_name = omdb_info.get("Genre")
                imdb_rating_str = omdb_info.get("imdbRating")
                imdb_votes_str = omdb_info.get("imdbVotes")

                try:
                    imdb_rating = float(imdb_rating_str) if imdb_rating_str != "N/A" else None
                except:
                    imdb_rating = None
                try:
                    imdb_votes = int(imdb_votes_str.replace(",", "")) if imdb_votes_str != "N/A" else None
                except:
                    imdb_votes = None

                genre_id = get_or_create_genre_id(genre_name)

                print(f"OMDb Data:")
                print(f"  • Genre        : {genre_name}")
                print(f"  • IMDb Rating  : {imdb_rating}")
                print(f"  • IMDb Votes   : {imdb_votes}")
            else:
                print("OMDb fetch returned None.")
        else:
            print("IMDb ID is not available; skipping OMDb fetch.")

        # Insert/update Movies table
        movie_id = insert_or_update_movie(
            title=title,
            release_year=release_year,
            genre_id=genre_id,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            popularity=popularity,
            vote_count=vote_count,
            average_vote=average_vote,
            budget=budget,
            imdb_rating=imdb_rating,
            imdb_votes=imdb_votes,
        )

        print(f"> Inserted/Updated Movie ID: {movie_id}")

        # Optionally fetch showtimes here
        # slots_count = fetch_showtime_slots(title)
        # insert_showtimes_data(movie_id, slots_count)

        time.sleep(0.3)  # Be kind to the APIs

    set_last_page_retrieved(next_page)
    print(f"\nSuccessfully processed page {next_page}.")

    print("\n==== DATABASE STATE AFTER RUN ====")
    print_database_state()


if __name__ == "__main__":
    main()