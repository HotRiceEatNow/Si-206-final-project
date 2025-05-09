import sqlite3
from datetime import date
from config import DB_PATH

def create_database():
    """
    Initializes the SQLite database:
    - Genres table mapping unique ID to a Genre string
    - Distributors table mapping unique ID to a Distributor string
    - Movies table giving details about each specific movie
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # -- Genres table (to avoid string duplication)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
        """
    )

    # -- Distributors table (to avoid string duplication)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Distributors (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT    UNIQUE
        )
        """
    )

    # -- Movies table (combined TMDb and OMDb data)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Movies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            title         TEXT,
            release_year  INTEGER,
            genre_id      INTEGER,
            tmdb_id       INTEGER UNIQUE,
            imdb_id       TEXT    UNIQUE,
            popularity    REAL,
            vote_count    INTEGER,
            average_vote  REAL,
            budget        INTEGER,
            imdb_rating   REAL,
            imdb_votes    INTEGER,
            gross         TEXT,
            theaters      INTEGER,
            total_gross   TEXT,
            distributor_id INTEGER,
            FOREIGN KEY(genre_id)      REFERENCES Genres(id),
            FOREIGN KEY(distributor_id) REFERENCES Distributors(id)
        )
        """
        # popularity, vote_count, average_vote, and budget are from TMDb API
        # imdb_rating, and imdb_votes are from OMDb API
    )

    conn.commit()
    conn.close()


def get_or_create_genre_id(genre_name):
    """
    Returns the genre_id from the Genres table.
    If the genre does not exist, inserts it and returns the new ID.
    """
    conn = sqlite3.connect(DB_PATH)
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


def get_or_create_distributor_id(name):
    """
    Returns the distributor’s ID, inserting into Distributors if needed.
    """
    if not name:
        return None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id FROM Distributors WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        dist_id = row[0]
    else:
        cur.execute("INSERT INTO Distributors (name) VALUES (?)", (name,))
        dist_id = cur.lastrowid
        conn.commit()
    conn.close()
    return dist_id


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
    Returns the movie_id.
    """
    conn = sqlite3.connect(DB_PATH)
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
                distributor_id = ?
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
                total_gross, distributor_id
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


def print_database_state():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("\n-- Genres Table --")
    cur.execute("SELECT id, name FROM Genres ORDER BY id")
    genres = cur.fetchall()
    if genres:
        for g in genres:
            print(f"  {g[0]}: {g[1]}")
    else:
        print("  (empty)")

    print("\n-- Distributors Table --")
    cur.execute("SELECT id, name FROM Distributors ORDER BY id")
    distributors = cur.fetchall()
    if distributors:
        for d in distributors:
            print(f"  {d[0]}: {d[1]}")
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
            distributor_id
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
    Distributor: {fmt_str(m[15])}
""")
    else:
        print("  (empty)")

    conn.close()
