import time

from config import LIMIT
from db import create_database, get_or_create_genre_id, insert_or_update_movie, print_database_state
from tmdb import fetch_tmdb_popular_movies, get_tmdb_movie_details, get_last_page_retrieved, set_last_page_retrieved
from omdb import fetch_omdb_data
from boxofficemojo import fetch_bom_data_for_title

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

        gross, theaters, total_gross, distributor = fetch_bom_data_for_title(title)
        print(f"  [BoxOfficeMojo] for '{title}': gross={gross}, theaters={theaters}, total_gross={total_gross}, distributor={distributor}")        

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
            gross=gross,
            theaters=theaters,
            total_gross=total_gross,
            distributor=distributor
        )
        print(f"> Inserted/Updated Movie ID: {movie_id}")

        # Fetch showtimes from Serp here
        # print(f"> Fetching showtimes for: '{title}'")
        # slots_count = fetch_showtime_slots(title)
        # insert_showtimes_data(movie_id, slots_count)

        time.sleep(0.3)  # Be kind to the APIs

    set_last_page_retrieved(next_page)
    print(f"\nSuccessfully processed page {next_page}.")

    print("\n==== DATABASE STATE AFTER RUN ====")
    print_database_state()

if __name__ == "__main__":
    main()