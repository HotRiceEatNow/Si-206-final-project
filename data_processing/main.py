import sqlite3
import os

DB_PATH = "../movies.db"
OUTPUT_DIR = "analysis_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def clean_gross(gross_str):
    if not gross_str or gross_str in ("", "N/A"):
        return None
    try:
        return int(gross_str.replace("$", "").replace(",", ""))
    except ValueError:
        return None


def fetch_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT title, release_year, budget, total_gross, imdb_rating 
        FROM Movies
        WHERE budget IS NOT NULL AND budget > 0 AND total_gross IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    cleaned_data = []
    for title, year, budget, gross, rating in rows:
        gross_int = clean_gross(gross)
        if gross_int is None or gross_int == 0:
            continue
        if rating is None or rating == 0:
            rating = None
        cleaned_data.append({
            "title": title,
            "year": year,
            "budget": budget,
            "gross": gross_int,
            "profit": gross_int - budget,
            "imdb_rating": rating
        })

    return cleaned_data


def save_profitability_txt(data):
    path = os.path.join(OUTPUT_DIR, "profitability.txt")
    with open(path, "w") as f:
        for row in sorted(data, key=lambda x: x["profit"], reverse=True):
            profit = row["profit"]
            action = "gained" if profit >= 0 else "lost"
            f.write(
                f"{row['title']} had a budget of ${row['budget']:,} and had a gross of ${row['gross']:,}, "
                f"so it {action} ${abs(profit):,}.\n"
            )


def save_rating_vs_revenue_txt(data):
    path = os.path.join(OUTPUT_DIR, "rating_vs_revenue.txt")
    filtered = [row for row in data if row["imdb_rating"] is not None]
    sorted_rows = sorted(filtered, key=lambda x: x["imdb_rating"])

    with open(path, "w") as f:
        for row in sorted_rows:
            f.write(
                f"{row['title']} had a rating of {row['imdb_rating']} and earned ${row['gross']:,}.\n"
            )


def main():
    print("Fetching and cleaning data...")
    data = fetch_data()

    print("Writing profitability report...")
    save_profitability_txt(data)

    print("Writing rating vs revenue report...")
    save_rating_vs_revenue_txt(data)

    print(f"\nAll reports saved in '{OUTPUT_DIR}/'.")


if __name__ == "__main__":
    main()