#!/usr/bin/env python3
"""Generate realistic, deliberately-imperfect sample data.

The defects are intentional and reproducible (fixed seed): they exercise the
quality rules, the quarantine path and schema evolution. Roughly 4% of order rows
carry a defect, which is about what a real upstream CSV feed looks like.

Usage:
    python scripts/generate_sample_data.py --out data/samples --days 14
    python scripts/generate_sample_data.py --out data/samples --days 3 --evolve
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42

CATEGORIES = {
    "Electronics": ["Laptops", "Phones", "Audio", "Wearables"],
    "Home": ["Kitchen", "Furniture", "Decor", "Bedding"],
    "Sports": ["Fitness", "Outdoor", "Cycling", "Team Sports"],
    "Books": ["Fiction", "Technical", "Children", "Business"],
    "Fashion": ["Menswear", "Womenswear", "Footwear", "Accessories"],
}
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Soylent", "Stark", "Wayne", "Tyrell"]
COUNTRIES = ["IN", "US", "GB", "DE", "AU", "SG", "AE", "CA"]
CITIES = {
    "IN": ["Hyderabad", "Bengaluru", "Mumbai", "Rajahmundry", "Chennai"],
    "US": ["Seattle", "Austin", "Boston", "Denver"],
    "GB": ["London", "Manchester", "Bristol"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "AU": ["Sydney", "Melbourne"],
    "SG": ["Singapore"],
    "AE": ["Dubai", "Abu Dhabi"],
    "CA": ["Toronto", "Vancouver"],
}
SEGMENTS = ["Consumer", "Corporate", "Home Office", "SMB", "Enterprise"]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
CHANNELS = ["web", "mobile_app", "marketplace", "retail_store", "phone"]
FIRST = [
    "Aarav",
    "Diya",
    "Rohan",
    "Ananya",
    "Vikram",
    "Meera",
    "Arjun",
    "Priya",
    "Sarah",
    "James",
    "Wei",
    "Fatima",
    "Lucas",
    "Emma",
    "Noah",
    "Olivia",
]
LAST = [
    "Sharma",
    "Reddy",
    "Patel",
    "Nair",
    "Rao",
    "Iyer",
    "Smith",
    "Johnson",
    "Chen",
    "Al-Rashid",
    "Muller",
    "Garcia",
    "Kim",
    "Okafor",
]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>6,} rows -> {path}")


def generate_products(n: int, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        cost = round(rng.uniform(5, 900), 2)
        price = round(cost * rng.uniform(1.15, 2.6), 2)
        rows.append(
            {
                "product_id": f"SKU-{i:05d}",
                "product_name": f"{rng.choice(BRANDS)} {rng.choice(CATEGORIES[category])} {i:04d}",
                "category": category,
                "subcategory": rng.choice(CATEGORIES[category]),
                "brand": rng.choice(BRANDS),
                "list_price": price,
                "unit_cost": cost,
                "is_active": rng.random() > 0.08,
                "updated_at": (
                    datetime(2026, 1, 1) + timedelta(days=rng.randint(0, 200))
                ).isoformat(sep=" "),
            }
        )
    # Defects: negative price, missing name, malformed id
    rows[3]["list_price"] = -12.50
    rows[7]["product_name"] = ""
    rows[11]["product_id"] = "SKU-BAD"
    return rows


def generate_customers(n: int, rng: random.Random, day: date) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(COUNTRIES)
        first, last = rng.choice(FIRST), rng.choice(LAST)
        rows.append(
            {
                "customer_id": f"CUST-{i:06d}",
                "full_name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{i}@example.com",
                "phone": f"+{rng.randint(1, 99)}{rng.randint(1000000000, 9999999999)}",
                "country": country,
                "city": rng.choice(CITIES[country]),
                "segment": rng.choice(SEGMENTS),
                "signup_date": (
                    date(2022, 1, 1) + timedelta(days=rng.randint(0, 1200))
                ).isoformat(),
                "lifetime_value": round(rng.uniform(0, 25000), 2),
                "updated_at": datetime.combine(day, datetime.min.time()).isoformat(sep=" "),
            }
        )
    # Defects: malformed email, bad country code, future signup
    rows[2]["email"] = "not-an-email"
    rows[5]["country"] = "INDIA"
    rows[9]["signup_date"] = (date.today() + timedelta(days=30)).isoformat()
    return rows


def generate_orders(
    n: int,
    rng: random.Random,
    day: date,
    n_customers: int,
    n_products: int,
    start_seq: int,
    evolve: bool = False,
) -> list[dict]:
    rows = []
    for i in range(n):
        seq = start_seq + i
        qty = rng.randint(1, 8)
        price = round(rng.uniform(8, 1500), 2)
        row = {
            "order_id": f"ORD-{seq:08d}",
            "customer_id": f"CUST-{rng.randint(1, n_customers):06d}",
            "product_id": f"SKU-{rng.randint(1, n_products):05d}",
            "quantity": qty,
            "unit_price": price,
            "discount_pct": rng.choice([0, 0, 0, 5, 10, 15, 20]),
            "status": rng.choices(STATUSES, weights=[5, 20, 25, 40, 7, 3])[0],
            "currency": rng.choices(["USD", "INR", "GBP", "EUR"], weights=[50, 30, 10, 10])[0],
            "channel": rng.choice(CHANNELS),
            "customer_email": f"user{rng.randint(1, n_customers)}@example.com",
            "order_date": day.isoformat(),
            "updated_at": datetime.combine(
                day, datetime.min.time().replace(hour=rng.randint(0, 23), minute=rng.randint(0, 59))
            ).isoformat(sep=" "),
        }
        if evolve:
            # Schema evolution: two NEW columns appear mid-stream. An additive
            # policy must absorb these without a pipeline failure.
            row["promotion_code"] = rng.choice(["", "SAVE10", "FREESHIP", "BF2026"])
            row["fulfilment_center"] = rng.choice(["FC-HYD-01", "FC-BLR-02", "FC-DEL-03"])
        rows.append(row)

    # ~4% deliberate defects, spread across rule types
    n_defects = max(1, int(n * 0.04))
    for idx in rng.sample(range(len(rows)), n_defects):
        defect = rng.choice(["qty", "price", "status", "currency", "id", "customer", "email"])
        if defect == "qty":
            rows[idx]["quantity"] = rng.choice([0, -3])
        elif defect == "price":
            rows[idx]["unit_price"] = -99.99
        elif defect == "status":
            rows[idx]["status"] = "UNKNOWN_STATE"
        elif defect == "currency":
            rows[idx]["currency"] = "Dollars"
        elif defect == "id":
            rows[idx]["order_id"] = f"BADORDER{idx}"
        elif defect == "customer":
            rows[idx]["customer_id"] = ""
        else:
            rows[idx]["customer_email"] = "broken@@example"

    # Late-arriving duplicate: same order_id, newer updated_at, different status.
    # Silver dedupe must keep exactly the newest version.
    if len(rows) > 20:
        dup = dict(rows[10])
        dup["status"] = "delivered"
        dup["updated_at"] = datetime.combine(
            day, datetime.min.time().replace(hour=23, minute=59)
        ).isoformat(sep=" ")
        rows.append(dup)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", default="data/samples", help="output directory")
    parser.add_argument("--days", type=int, default=14, help="days of order history")
    parser.add_argument("--orders-per-day", type=int, default=400)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument(
        "--evolve",
        action="store_true",
        help="emit an extra day with NEW columns to exercise schema evolution",
    )
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    start_day = date.today() - timedelta(days=args.days)

    print(f"Generating sample data into {out}/ (seed={args.seed})")

    products = generate_products(args.products, rng)
    _write_csv(out / "products" / "products.csv", products, list(products[0].keys()))

    customers = generate_customers(args.customers, rng, start_day)
    _write_csv(
        out / "customers" / f"customers_{start_day.isoformat()}.csv",
        customers,
        list(customers[0].keys()),
    )

    # SCD2 driver: a later file updates a subset of customers (segment/city change).
    changed = [dict(c) for c in customers[:40]]
    change_day = start_day + timedelta(days=max(1, args.days // 2))
    for c in changed:
        c["segment"] = rng.choice(SEGMENTS)
        c["city"] = rng.choice(CITIES.get(c["country"], ["Unknown"]))
        c["updated_at"] = datetime.combine(change_day, datetime.min.time()).isoformat(sep=" ")
    _write_csv(
        out / "customers" / f"customers_{change_day.isoformat()}.csv",
        changed,
        list(changed[0].keys()),
    )

    seq = 1
    for d in range(args.days):
        day = start_day + timedelta(days=d)
        n = int(args.orders_per_day * rng.uniform(0.7, 1.3))
        rows = generate_orders(n, rng, day, args.customers, args.products, seq)
        _write_csv(out / "orders" / f"orders_{day.isoformat()}.csv", rows, list(rows[0].keys()))
        seq += n

    if args.evolve:
        day = start_day + timedelta(days=args.days)
        rows = generate_orders(
            args.orders_per_day, rng, day, args.customers, args.products, seq, evolve=True
        )
        _write_csv(out / "orders" / f"orders_{day.isoformat()}.csv", rows, list(rows[0].keys()))
        print("  ^ this file adds promotion_code + fulfilment_center (schema evolution)")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
