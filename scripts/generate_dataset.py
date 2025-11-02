import argparse
import csv
import os
import random
from datetime import datetime, timedelta


def generate_rows(n_rows: int, skew: float = 0.2):
    random.seed(42)
    base_time = datetime(2024, 1, 1)
    categories = ["alpha", "beta", "gamma", "delta", "epsilon"]

    # Zipf-like skew for user_id distribution
    hot_users = max(1, int(n_rows * skew / 1000))
    for i in range(n_rows):
        if random.random() < skew:
            user_id = f"u_{random.randint(1, hot_users)}"
        else:
            user_id = f"u_{random.randint(1, max(2, n_rows // 1000))}"
        event_ts = base_time + timedelta(seconds=random.randint(0, 90 * 24 * 3600))
        category = random.choice(categories)
        amount = round(random.random() * 100, 2)
        yield [user_id, event_ts.isoformat(), category, amount]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--out", type=str, default="data")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    out_file = os.path.join(args.out, "events.csv")

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "event_ts", "category", "amount"])
        for row in generate_rows(args.rows):
            writer.writerow(row)

    print(f"Wrote {args.rows} rows to {out_file}")


if __name__ == "__main__":
    main()
