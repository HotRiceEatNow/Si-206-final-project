# File to gather data from the different various sources
# (Box Office Mojo via BeautifulSoup, TMDb API, OMDb API, SerpAPI)

import sqlite3
import os
import requests
import time
from bs4 import BeautifulSoup


DB_NAME = "movies.db"

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "ab67f773b96f6a5c2a52209b77fdd8b5")
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "1ad61f09")
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY", "ec44919024d8492b657c179a691b512169778a1e5d8c4f34ae2d2738db9d6415")

LIMIT = 5


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
            gross TEXT,
            theaters INTEGER,
            total_gross TEXT,
            distributor TEXT,
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
    release_year=None,
    genre_id=None,
    tmdb_id=None,
    imdb_id=None,
    popularity=None,
    vote_count=None,
    average_vote=None,
    budget=None,
    imdb_rating=None,
    imdb_votes=None,
    gross=None,
    theaters=None,
    total_gross=None,
    distributor=None
):
    """
    Insert a new movie or update an existing one based on tmdb_id or title.
    Handles extended fields including gross, theaters, total_gross, distributor.
    Returns the movie_id.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Try match on tmdb_id first, fallback to title
    if tmdb_id:
        cur.execute("SELECT id FROM Movies WHERE tmdb_id = ?", (tmdb_id,))
    else:
        cur.execute("SELECT id FROM Movies WHERE title = ?", (title,))
    row = cur.fetchone()

    if row:
        movie_id = row[0]
        cur.execute(
            '''
            UPDATE Movies SET
                title = ?,
                release_year = ?,
                genre_id = ?,
                tmdb_id = ?,
                imdb_id = ?,
                popularity = ?,
                vote_count = ?,
                average_vote = ?,
                budget = ?,
                imdb_rating = ?,
                imdb_votes = ?,
                gross = ?,
                theaters = ?,
                total_gross = ?,
                distributor = ?
            WHERE id = ?
            ''',
            (title, release_year, genre_id, tmdb_id, imdb_id,
             popularity, vote_count, average_vote, budget,
             imdb_rating, imdb_votes, gross, theaters,
             total_gross, distributor, movie_id)
        )
    else:
        cur.execute(
            '''
            INSERT INTO Movies (
                title, release_year, genre_id, tmdb_id, imdb_id,
                popularity, vote_count, average_vote, budget,
                imdb_rating, imdb_votes, gross, theaters,
                total_gross, distributor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (title, release_year, genre_id, tmdb_id, imdb_id,
             popularity, vote_count, average_vote, budget,
             imdb_rating, imdb_votes, gross, theaters,
             total_gross, distributor)
        )
        movie_id = cur.lastrowid

    conn.commit()
    conn.close()
    return movie_id



# def fetch_showtime_slots(movie_title):
#     """
#     Calls SerpApi's Showtimes endpoint to scrape a movie's showtimes from Google.
#     Returns an integer count of how many times the movie is playing (slots_count) or 0 if none found or if there's an error.
#     """
#     today_str = date.today().isoformat()
#     query_str = f"{movie_title} showtimes near me"

#     print(f"  [fetch_showtime_slots] Querying SerpApi for '{movie_title}' on {today_str}")

#     url = "https://serpapi.com/search.json"
#     params = {
#         "engine": "google_showtimes",
#         "q": query_str,
#         "hl": "en",
#         "gl": "us",
#         "location": "New York,NY,USA",
#         "start_date": today_str,
#         "end_date": today_str,
#         "movie_times": "1",
#         "api_key": SERPAPI_API_KEY
#     }

#     response = requests.get(url, params=params)

#     if response.status_code != 200:
#         print(f"  [ERROR] SerpApi returned status {response.status_code} for '{movie_title}'")
#         return 0
#     else:
#         print(f"  [fetch_showtime_slots] Got response for '{movie_title}' (status {response.status_code})")

#     data = response.json()

#     # print(json.dumps(data, indent=2))

#     showtimes_results = data.get("showtimes", [])
#     total_slots = 0

#     for theater_info in showtimes_results:
#         for show_date_info in theater_info.get("showing", []):
#             times_list = show_date_info.get("times", [])
#             total_slots += len(times_list)

#     print(f"  [fetch_showtime_slots] Found {total_slots} showtime slots for '{movie_title}'")
#     return total_slots


# def insert_showtimes_data(movie_id, slots_count):
#     """
#     Inserts or updates the daily showtimes count in the Showtimes table
#     for the given movie_id. We'll store the date as 'today'.
#     """
#     conn = sqlite3.connect(DB_NAME)
#     cur = conn.cursor()

#     today_str = date.today().isoformat()
#     print(f"  [insert_showtimes_data] Storing {slots_count} slots for movie_id={movie_id} on {today_str}")

#     cur.execute(
#         "SELECT id FROM Showtimes WHERE movie_id = ? AND show_date = ?",
#         (movie_id, today_str)
#     )
#     row = cur.fetchone()

#     if row:
#         cur.execute(
#             """
#             UPDATE Showtimes
#             SET slots_count = ?
#             WHERE movie_id = ? AND show_date = ?
#             """,
#             (slots_count, movie_id, today_str)
#         )
#         print(f"  [DB] Updated existing showtime record for movie_id={movie_id}")
#     else:
#         cur.execute(
#             """
#             INSERT INTO Showtimes (movie_id, show_date, slots_count)
#             VALUES (?, ?, ?)
#             """,
#             (movie_id, today_str, slots_count)
#         )
#         print(f"  [DB] Inserted new showtime record for movie_id={movie_id}")

#     conn.commit()
#     conn.close()

URL = "https://www.boxofficemojo.com/year/2025/"

def fetch_html(url):
    """Fetch HTML content from a given URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_html(html):
    """Parse HTML using BeautifulSoup and return the soup object."""
    return BeautifulSoup(html, 'html.parser')

def extract_table(soup):
    """Extract the movie table element from the parsed soup."""
    return soup.select_one('div#table table')

def normalize_cell(text):
    """Normalize cell value: convert '-' to None, strip text otherwise."""
    stripped = text.strip()
    return None if stripped == "-" else stripped

def parse_movie_row(row):
    """Parse a single row of movie data and return it as a dictionary."""
    cells = row.find_all('td')
    if len(cells) < 10:
        return None

    # Extract and normalize fields
    release_title = normalize_cell(cells[1].get_text())
    gross = normalize_cell(cells[5].get_text())
    theaters = normalize_cell(cells[6].get_text())
    total_gross = normalize_cell(cells[7].get_text())
    release_date = normalize_cell(cells[8].get_text())
    distributor = normalize_cell(cells[9].get_text())

    # If any field is None (i.e., a NULL value), disregard this entire row
    if None in [release_title, gross, theaters, total_gross, release_date, distributor]:
        return None

    return {
        "Release Title": release_title,
        "Gross": gross,
        "Theaters": theaters,
        "Total Gross": total_gross,
        "Release Date": release_date,
        "Distributor": distributor,
    }

def extract_movies(table):
    """Extract all movies from the table."""
    rows = table.find_all('tr')[1:]
    movies = []
    for row in rows:
        movie = parse_movie_row(row)
        if movie:
            movies.append(movie)
    
    return movies


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
    # select all of the columns in your updated schema
    cur.execute("""
        SELECT
            id,
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
            gross,
            theaters,
            total_gross,
            distributor
        FROM Movies
        ORDER BY id DESC
    """)
    movies = cur.fetchall()

    def fmt_num(val, fmt_str="{:,}"):
        return fmt_str.format(val) if val is not None else "None"

    def fmt_str(val):
        return val if val is not None else "None"

    if movies:
        for m in movies:
            print(f"""  {m[0]}: {m[1]} ({m[2]})
    Genre ID   : {fmt_num(m[3])}
    TMDb ID    : {fmt_num(m[4])}   IMDb ID : {fmt_str(m[5])}
    Popularity : {fmt_num(m[6], "{:.2f}")}   Vote Count : {fmt_num(m[7])}   Avg Vote : {fmt_num(m[8], "{:.1f}")}
    Budget     : ${fmt_num(m[9])}   IMDb Rating : {fmt_num(m[10], "{:.1f}")}   IMDb Votes : {fmt_num(m[11])}
    Gross      : {fmt_str(m[12])}
    Theaters   : {fmt_str(m[13])}
    Total Gross: {fmt_str(m[14])}
    Distributor : {fmt_str(m[15])}
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
    movies_to_process = movies[:LIMIT]

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

        # Fetch showtimes from Serp here
        # print(f"> Fetching showtimes for: '{title}'")
        # slots_count = fetch_showtime_slots(title)
        # insert_showtimes_data(movie_id, slots_count)

        time.sleep(0.3)  # Be kind to the APIs

    set_last_page_retrieved(next_page)
    print(f"\nSuccessfully processed page {next_page}.")

    # -- Box Office Mojo scraping pipeline
    print("\nFetching Box Office Mojo top movies for 2025…")
    html  = fetch_html(URL)
    soup  = parse_html(html)
    table = extract_table(soup)
    if not table:
        print("Could not find the Box Office Mojo table.")
    else:
        scraped = extract_movies(table)
        to_add  = scraped[:LIMIT]
        print(f"Found {len(scraped)} movies on BOM, processing up to {len(to_add)} titles:")

        for idx, info in enumerate(to_add, 1):
            print(f"{idx:>2}. {info['Release Title']}")

        for info in to_add:
            title       = info["Release Title"]
            gross       = info["Gross"]
            theaters    = info["Theaters"]
            total_gross = info["Total Gross"]
            distributor = info["Distributor"]

            conn = sqlite3.connect(DB_NAME)
            cur  = conn.cursor()

            # Check if already in DB
            cur.execute("SELECT id FROM Movies WHERE title = ?", (title,))
            row = cur.fetchone()
            if row:
                movie_id = row[0]
                # Update only the BOM fields
                cur.execute("""
                    UPDATE Movies
                    SET gross       = ?,
                        theaters    = ?,
                        total_gross = ?,
                        distributor = ?
                    WHERE id = ?
                """, (gross, theaters, total_gross, distributor, movie_id))
                print(f"  [UPDATE] '{title}' (id={movie_id}): gross→{gross}, theaters→{theaters}, total_gross→{total_gross}, distributor→{distributor}")
            else:
                # Insert a fresh row with just the BOM fields (others stay NULL)
                cur.execute("""
                    INSERT INTO Movies
                        (title, gross, theaters, total_gross, distributor)
                    VALUES (?, ?, ?, ?, ?)
                """, (title, gross, theaters, total_gross, distributor))
                movie_id = cur.lastrowid
                print(f"  [INSERT] '{title}' as id={movie_id}")

            conn.commit()
            conn.close()

    print("\n==== DATABASE STATE AFTER RUN ====")
    print_database_state()


if __name__ == "__main__":
    main()