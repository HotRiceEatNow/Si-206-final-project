import requests
from config import BOXOFFICEMOJO_URL
from bs4 import BeautifulSoup

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


def fetch_bom_data_for_title(title: str):
    """
    Scrape the BOM page and return a 4-tuple of
      (gross, theaters, total_gross, distributor)
    for the given movie title, or (None, None, None, None) if not found.
    """
    html = fetch_html(BOXOFFICEMOJO_URL)
    soup = parse_html(html)
    table = extract_table(soup)
    if not table:
        return (None, None, None, None)

    for row in table.find_all('tr')[1:]:
        info = parse_movie_row(row)
        if info and info["Release Title"] == title:
            return (
                info["Gross"],
                info["Theaters"],
                info["Total Gross"],
                info["Distributor"],
            )

    return (None, None, None, None)
