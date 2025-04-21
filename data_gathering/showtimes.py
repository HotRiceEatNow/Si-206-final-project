import requests
from datetime import date
from config import SERPAPI_API_KEY, debug_print

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
        debug_print(f"Error fetching showtime data from SerpApi for '{movie_title}' (status {response.status_code})")
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
