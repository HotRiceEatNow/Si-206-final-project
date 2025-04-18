# File to gather data from the different various sources
# (Box Office Mojo via BeautifulSoup, TMDb API, OMDb API, SerpAPI)

import sqlite3
import os

DB_NAME = "movies.db"

def create_database():
    """
    Initializes the SQLite database:
    - Genres table mapping unique ID to a Genre string
    - Movies table giving details about each specific movie
    - ShowtimesData table which is a one-to-many relationship
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

    # -- ShowtimesData table
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


def main():
    # create SQLite database if it does not already exist
    create_database()

    last_page = get_last_page_retrieved()
    next_page = last_page + 1

    print(f"Fetching up to 25 new popular movies from TMDb (page {next_page}) ...")
    # use the TMDb API as a baseline from which we can gather data from other sources
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

        # Initialize genre-related fields
        genre_id = None
        imdb_rating = None
        imdb_votes = None

        # If IMDb ID is available, fetch OMDb
        if imdb_id:
            omdb_info = fetch_omdb_data(imdb_id)
            if omdb_info is not None:
                genre_name = omdb_info["Genre"]
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

                # Insert genre (if new) and get genre_id
                genre_id = get_or_create_genre_id(genre_name)

        # Insert or update Movies table with full metadata
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

        # Fetch SerpApi showtimes (slots_count) for this movie title
        slots_count = fetch_showtime_slots(title)
        insert_showtimes_data(movie_id, slots_count)

        # Be kind to the APIs
        time.sleep(0.3)

    set_last_page_retrieved(next_page)
    print(f"Successfully processed page {next_page}. Run again to fetch some more data.")
    show_data()

