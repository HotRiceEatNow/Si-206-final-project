import sqlite3
import os
import matplotlib.pyplot as plt

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

    return rows


def save_profitability_txt(data):
    path = os.path.join(OUTPUT_DIR, "profitability.txt")
    valid_rows = []

    for row in data:
        title, year, budget, gross, rating = row
        gross_clean = clean_gross(gross)

        if budget is None or budget <= 0 or gross_clean is None or gross_clean == 0:
            continue

        profit = gross_clean - budget
        valid_rows.append({
            "title": title,
            "budget": budget,
            "gross": gross_clean,
            "profit": profit
        })

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

        if budget is None or budget <= 0 or gross_clean is None or gross_clean == 0 or rating is None or rating == 0:
            continue

        profit = gross_clean - budget
        valid_rows.append({
            "title": title,
            "rating": rating,
            "profit": profit
        })

    sorted_rows = sorted(valid_rows, key=lambda x: x["rating"])

    with open(path, "w") as f:
        for row in sorted_rows:
            action = "gained" if row["profit"] >= 0 else "lost"
            f.write(
                f"{row['title']} had a rating of {row['rating']} and {action} ${abs(row['profit']):,}.\n"
            )


def plot_profitability_bar_chart(data):
    movie_data = []

    for row in data:
        title, _, budget, gross, _ = row
        gross_clean = clean_gross(gross)
        if budget is None or budget <= 0 or gross_clean is None or gross_clean == 0:
            continue

        profit = gross_clean - budget
        movie_data.append({
            "title": title,
            "budget": budget / 1_000_000,
            "gross": gross_clean / 1_000_000,
            "profit": profit
        })

    # Sort movies by profit (high to low)
    movie_data.sort(key=lambda x: x["profit"], reverse=True)

    titles = [movie["title"] for movie in movie_data]
    budgets = [movie["budget"] for movie in movie_data]
    grosses = [movie["gross"] for movie in movie_data]

    x = range(len(titles))
    width = 0.3

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width/2 for i in x], budgets, width=width, label="Budget", color="skyblue")
    ax.bar([i + width/2 for i in x], grosses, width=width, label="Gross", color="seagreen")

    ax.set_xticks(x)
    ax.set_xticklabels(titles, rotation=45, ha='right', fontsize=8)
    ax.set_ylabel("Amount (in millions of $)")
    ax.set_title("Movie Budgets vs Gross Earnings (Sorted by Profit/Deficit)")

    max_val = max(budgets + grosses)
    ax.set_ylim(0, max_val * 1.1)

    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "profitability_bar_chart_sorted.png"))
    plt.close()


def plot_rating_vs_revenue_scatter(data):
    ratings, profits = [], []

    for row in data:
        _, _, budget, gross, rating = row
        gross_clean = clean_gross(gross)
        if budget is None or budget <= 0 or gross_clean is None or gross_clean == 0 or rating is None or rating == 0:
            continue

        profit = (gross_clean - budget) / 1_000_000  # Convert to millions
        ratings.append(rating)
        profits.append(profit)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axhline(0, color='gray', linestyle='--', linewidth=1)
    scatter = ax.scatter(ratings, profits, c=["green" if p >= 0 else "red" for p in profits], alpha=0.6)

    ax.set_xlabel("IMDb Rating (on a scale from 1–10)")
    ax.set_ylabel("Profit (in millions of $)")
    ax.set_title("IMDb Rating vs Profitability")

    ax.set_xlim(1, 10)
    ax.set_xticks(range(1, 11))

    # Set y-axis limits based on data range
    max_profit = max(profits)
    min_profit = min(profits)
    ax.set_ylim(min_profit * 1.1, max_profit * 1.1)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "rating_vs_revenue_scatter.png"))
    plt.close()


def main():
    print("Fetching and cleaning data...")
    data = fetch_data()

    print("Writing profitability report...")
    save_profitability_txt(data)

    print("Writing rating vs revenue report...")
    save_rating_vs_revenue_txt(data)

    print("Creating profitability bar chart...")
    plot_profitability_bar_chart(data)

    print("Creating rating vs revenue scatter plot...")
    plot_rating_vs_revenue_scatter(data)

    print(f"\nAll reports and visualizations saved in '{OUTPUT_DIR}/'.")


if __name__ == "__main__":
    main()
