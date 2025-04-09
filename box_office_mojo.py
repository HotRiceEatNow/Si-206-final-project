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

def normalize_cell(text):
    """Normalize cell value: convert '-' to None, strip text otherwise."""
    stripped = text.strip()
    return None if stripped == "-" else stripped

def parse_movie_row(row):
    """Parse a single row of movie data and return it as a dictionary."""
    cells = row.find_all('td')
    if len(cells) < 10:
        return None

    return {
        "Release Title": normalize_cell(cells[1].get_text()),
        "Gross": normalize_cell(cells[5].get_text()),
        "Theaters": normalize_cell(cells[6].get_text()),
        "Total Gross": normalize_cell(cells[7].get_text()),
        "Release Date": normalize_cell(cells[8].get_text()),
        "Distributor": normalize_cell(cells[9].get_text()),
    }

def extract_movies(table):
    """Extract all movies from the table."""
    rows = table.find_all('tr')[1:]
    return [movie for row in rows if (movie := parse_movie_row(row))]

def main():
    html = fetch_html(URL)
    soup = parse_html(html)
    table = extract_table(soup)
    if not table:
        print("Could not find the movie table.")
        return []

    movies = extract_movies(table)
    for i, movie in enumerate(movies[:5], start=1):
        print(f"{i}. {movie}")
    return movies

if __name__ == "__main__":
    all_movies = main()
