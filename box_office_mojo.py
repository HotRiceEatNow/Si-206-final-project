# Python file to web scrape data from the BoxOfficeMojo website
# https://www.boxofficemojo.com/year/2025/

import requests
from bs4 import BeautifulSoup

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

def parse_movie_row(row):
    """Parse a single row of movie data and return it as a dictionary."""
    cells = row.find_all('td')
    if len(cells) < 10:
        return None

    # TODO: some fields in the table on the website have "--" for NULL

    return {
        "Release Title": cells[1].get_text(strip=True),
        "Gross": cells[5].get_text(strip=True),
        "Theaters": cells[6].get_text(strip=True),
        "Total Gross": cells[7].get_text(strip=True),
        "Release Date": cells[8].get_text(strip=True),
        "Distributor": cells[9].get_text(strip=True),
    }

def extract_movies(table):
    """Extract all movies from the table."""
    rows = table.find_all('tr')[1:]  # Skip the header row
    movies = []
    for row in rows:
        movie = parse_movie_row(row)
        if movie:
            movies.append(movie)
    return movies

def main():
    html = fetch_html(URL)
    soup = parse_html(html)
    table = extract_table(soup)
    if not table:
        print("Could not find the movie table.")
        return

    movies = extract_movies(table)
    for i, movie in enumerate(movies[:5], start=1):
        print(f"{i}. {movie}")

    # Return dictionary of all movie data
    # TODO: write to persistent DB storage instead of in-memory dictionary
    return movies

if __name__ == "__main__":
    all_movies = main()
