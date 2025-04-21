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
    """)
    rows = cur.fetchall()
    conn.close()

    # Return raw tuples
    return rows


def save_profitability_txt(data):
    path = os.path.join(OUTPUT_DIR, "profitability.txt")
    valid_rows = []

    for row in data:
        title, year, budget, gross, rating = row
        gross_clean = clean_gross(gross)

        if budget is None or budget <= 0:
            continue
        if gross_clean is None or gross_clean == 0:
            continue

        profit = gross_clean - budget
        valid_rows.append({
            "title": title,
            "budget": budget,
            "gross": gross_clean,
            "profit": profit
        })

    # Sort from highest profit to biggest loss
    sorted_rows = sorted(valid_rows, key=lambda x: x["profit"], reverse=True)

    with open(path, "w") as f:
        for row in sorted_rows:
            action = "gained" if row["profit"] >= 0 else "lost"
            f.write(
                f"{row['title']} had a budget of ${row['budget']:,} and had a gross of ${row['gross']:,}, "
                f"so it {action} ${abs(row['profit']):,}.\n"
            )


def save_rating_vs_revenue_txt(data):
    path = os.path.join(OUTPUT_DIR, "rating_vs_revenue.txt")
    valid_rows = []

    for row in data:
        title, year, budget, gross, rating = row
        gross_clean = clean_gross(gross)

        if budget is None or budget <= 0:
            continue
        if gross_clean is None or gross_clean == 0:
            continue
        if rating is None or rating == 0:
            continue

        profit = gross_clean - budget
        valid_rows.append({
            "title": title,
            "rating": rating,
            "profit": profit
        })

    # Sort by increasing rating
    sorted_rows = sorted(valid_rows, key=lambda x: x["rating"])

    with open(path, "w") as f:
        for row in sorted_rows:
            action = "gained" if row["profit"] >= 0 else "lost"
            f.write(
                f"{row['title']} had a rating of {row['rating']} and {action} ${abs(row['profit']):,}.\n"
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