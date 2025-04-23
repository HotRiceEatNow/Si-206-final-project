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

        if budget is None or budget <= 0 or gross_clean is None or gross_clean <= 0:
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


def save_rating_vs_profit_txt(data):
    path = os.path.join(OUTPUT_DIR, "rating_vs_profit.txt")
    valid_rows = []

    for row in data:
        title, year, budget, gross, rating = row
        gross_clean = clean_gross(gross)

        if budget is None or budget <= 0 or gross_clean is None or gross_clean <= 0 or rating is None or rating <= 0:
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


def save_average_profitability_per_distributor_txt():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT Distributors.name, Movies.budget, Movies.total_gross
        FROM Movies
        JOIN Distributors ON Movies.distributor_id = Distributors.id
        WHERE Movies.budget IS NOT NULL AND Movies.budget > 0
          AND Movies.total_gross IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    distributor_stats = {}

    for name, budget, gross in rows:
        try:
            gross_int = int(str(gross).replace("$", "").replace(",", ""))
            profit = gross_int - budget
        except:
            continue

        if name not in distributor_stats:
            distributor_stats[name] = {"total_profit": 0, "num_movies": 0}
        distributor_stats[name]["total_profit"] += profit
        distributor_stats[name]["num_movies"] += 1

    path = os.path.join(OUTPUT_DIR, "distributor_avg_profitability.txt")
    with open(path, "w") as f:
        for distributor, stats in sorted(distributor_stats.items(), key=lambda x: x[1]["total_profit"]/x[1]["num_movies"], reverse=True):
            avg_profit = stats["total_profit"] / stats["num_movies"]
            action = "gained" if avg_profit >= 0 else "lost"
            f.write(
                f"{distributor} released {stats['num_movies']} movies in our dataset and on average {action} ${abs(avg_profit):,.2f} per movie.\n"
            )


def plot_profitability_bar_chart(data):
    movie_data = []

    for row in data:
        title, _, budget, gross, _ = row
        gross_clean = clean_gross(gross)
        if budget is None or budget <= 0 or gross_clean is None or gross_clean <= 0:
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


def plot_rating_vs_profit_scatter(data):
    ratings, profits = [], []

    for row in data:
        _, _, budget, gross, rating = row
        gross_clean = clean_gross(gross)
        if budget is None or budget <= 0 or gross_clean is None or gross_clean <= 0 or rating is None or rating <= 0:
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
    plt.savefig(os.path.join(OUTPUT_DIR, "rating_vs_profit_scatter.png"))
    plt.close()


def plot_distributor_avg_profitability_bar_chart():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT Distributors.name, Movies.budget, Movies.total_gross
        FROM Movies
        JOIN Distributors ON Movies.distributor_id = Distributors.id
        WHERE Movies.budget IS NOT NULL AND Movies.budget > 0
          AND Movies.total_gross IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    distributor_stats = {}

    for name, budget, gross in rows:
        try:
            gross_int = int(str(gross).replace("$", "").replace(",", ""))
            profit = gross_int - budget
        except:
            continue

        if name not in distributor_stats:
            distributor_stats[name] = {"total_profit": 0, "num_movies": 0}
        distributor_stats[name]["total_profit"] += profit
        distributor_stats[name]["num_movies"] += 1

    distributor_avg_profits = {
        name: stats["total_profit"] / stats["num_movies"]
        for name, stats in distributor_stats.items()
    }

    # Sort by average profit
    sorted_items = sorted(distributor_avg_profits.items(), key=lambda x: x[1], reverse=True)
    distributors = [item[0] for item in sorted_items]
    avg_profits_millions = [item[1] / 1_000_000 for item in sorted_items]

    plt.figure(figsize=(12, max(6, len(distributors) * 0.3)))
    bars = plt.barh(distributors, avg_profits_millions,
                    color=["green" if p >= 0 else "red" for p in avg_profits_millions])
    plt.xlabel("Average Profit per Movie (in millions of $)")
    plt.title("Average Movie Profit by Distributor")
    plt.axvline(0, color='black', linewidth=0.8)
    plt.tight_layout()
    plt.savefig("analysis_outputs/distributor_avg_profitability_bar_chart.png")
    plt.close()


# EXTRA CREDIT
def plot_release_year_vs_imdb_rating(data):
    years = []
    ratings = []

    for title, release_year, budget, total_gross, imdb_rating in data:
        if release_year is not None and imdb_rating is not None:
            years.append(release_year)
            ratings.append(imdb_rating)

    plt.figure(figsize=(12, 6))
    plt.scatter(years, ratings, alpha=0.6, color='blue', edgecolors='k')
    plt.xlabel("Release Year")
    plt.ylabel("IMDb Rating (on a scale from 1–10)")
    plt.title("Release Year vs IMDb Rating")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig("analysis_outputs/release_year_vs_imdb_rating.png")
    plt.close()


# EXTRA CREDIT
def plot_genre_vs_average_profitability():
    import sqlite3
    import matplotlib.pyplot as plt

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT Genres.name, Movies.budget, Movies.total_gross
        FROM Movies
        JOIN Genres ON Movies.genre_id = Genres.id
        WHERE Movies.budget IS NOT NULL AND Movies.budget > 0
          AND Movies.total_gross IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    genre_stats = {}

    for genre, budget, gross in rows:
        try:
            gross_int = int(str(gross).replace("$", "").replace(",", ""))
            profit = gross_int - budget
        except:
            continue

        if genre not in genre_stats:
            genre_stats[genre] = {"total_profit": 0, "num_movies": 0}
        genre_stats[genre]["total_profit"] += profit
        genre_stats[genre]["num_movies"] += 1

    genre_avg_profits = {
        genre: stats["total_profit"] / stats["num_movies"]
        for genre, stats in genre_stats.items()
    }

    # Convert to millions of dollars
    sorted_data = sorted(genre_avg_profits.items(), key=lambda x: x[1], reverse=True)
    genres = [item[0] for item in sorted_data]
    avg_profits_millions = [item[1] / 1e6 for item in sorted_data]

    # Plot
    plt.figure(figsize=(14, 6))
    bars = plt.bar(genres, avg_profits_millions, color='lightcoral', edgecolor='black')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel("Average Profit (in millions of $)")
    plt.title("Average Profitability by Genre")
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig("analysis_outputs/genre_vs_avg_profitability.png")
    plt.close()


def main():
    print("Fetching and cleaning data...")
    data = fetch_data()

    print("Writing profitability report...")
    save_profitability_txt(data)

    print("Writing rating vs revenue report...")
    save_rating_vs_profit_txt(data)

    print("Writing average profitability per distributor report...")
    save_average_profitability_per_distributor_txt()

    print("Creating profitability bar chart...")
    plot_profitability_bar_chart(data)

    print("Creating rating vs revenue scatter plot...")
    plot_rating_vs_profit_scatter(data)

    print("Creating average profitability per distributor bar chart...")
    plot_distributor_avg_profitability_bar_chart()

    # EXTRA CREDIT - VISUALIZATION 1
    print("Creating release year vs rating scatter plot...")
    plot_release_year_vs_imdb_rating(data)

    # EXTRA CREDIT - VISUALIZATION 2
    print("Creating genre vs average profitability bar chart...")
    plot_genre_vs_average_profitability()

    print(f"\nAll reports and visualizations saved in '{OUTPUT_DIR}/'.")


if __name__ == "__main__":
    main()
