# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Synthetic transaction feed generator
# MAGIC
# MAGIC Generates card transactions for ~2,000 customers as **daily JSONL files** in the
# MAGIC raw volume, so Auto Loader downstream sees genuinely incremental file arrivals.
# MAGIC
# MAGIC Built from scratch — no external dataset. Three fraud patterns are injected
# MAGIC (~0.4% of rows), plus deliberately dirty records (duplicates, nulls, malformed
# MAGIC amounts) for the Silver layer's expectations to catch.
# MAGIC
# MAGIC **Widgets:** `start_date` (first day to generate), `num_days` (how many days).
# MAGIC First run: 30 days of history. Later, the daily job reruns this with `num_days=1`
# MAGIC to simulate a live feed. Already-existing day files are skipped, so reruns are safe.

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-06-01")
dbutils.widgets.text("num_days", "30")

# COMMAND ----------

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta

SEED = 42
RAW_DIR = "/Volumes/workspace/fraud_lakehouse/raw/transactions"

N_CUSTOMERS = 2000
N_MERCHANTS = 300
TXNS_PER_DAY = 8000          # legitimate baseline volume
FRAUD_EPISODES_PER_DAY = 6   # each episode = several fraudulent transactions

COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AE", "AU", "JP"]
FOREIGN = ["RU", "NG", "BR", "VN", "RO", "PH"]
CATEGORIES = ["grocery", "fuel", "dining", "electronics", "travel",
              "utilities", "entertainment", "fashion", "health", "online_services"]

rng = random.Random(SEED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static world: customers and merchants
# MAGIC
# MAGIC Seeded, so every run regenerates the identical population — customer profiles
# MAGIC stay consistent across daily incremental runs.

# COMMAND ----------

customers = []
for i in range(N_CUSTOMERS):
    customers.append({
        "customer_id": f"C{i:05d}",
        "home_country": rng.choices(COUNTRIES, weights=[40, 15, 10, 8, 8, 7, 6, 6])[0],
        "avg_amount": rng.lognormvariate(3.6, 0.7),          # typical spend ~₹/$ 25-120
        "txns_per_day": rng.uniform(0.5, 8.0),
        "night_owl": rng.random() < 0.15,
        "devices": [f"D{uuid.UUID(int=rng.getrandbits(128)).hex[:10]}" for _ in range(rng.randint(1, 3))],
        "favorite_merchants": rng.sample(range(N_MERCHANTS), rng.randint(3, 10)),
    })

merchants = []
for i in range(N_MERCHANTS):
    merchants.append({
        "merchant_id": f"M{i:04d}",
        "category": rng.choice(CATEGORIES),
        "online": rng.random() < 0.4,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction builders

# COMMAND ----------

def txn_hour(cust, r):
    """Diurnal pattern: most spend 8:00-22:00; night owls skew later."""
    if cust["night_owl"] and r.random() < 0.4:
        return r.randint(0, 5)
    return int(min(23, max(0, r.gauss(14, 4))))


def legit_txn(cust, day, r):
    m = merchants[r.choice(cust["favorite_merchants"]) if r.random() < 0.7
                  else r.randrange(N_MERCHANTS)]
    ts = datetime.combine(day, datetime.min.time()) + timedelta(
        hours=txn_hour(cust, r), minutes=r.randint(0, 59), seconds=r.randint(0, 59))
    return {
        "transaction_id": str(uuid.UUID(int=r.getrandbits(128))),
        "event_ts": ts.isoformat(),
        "customer_id": cust["customer_id"],
        "merchant_id": m["merchant_id"],
        "merchant_category": m["category"],
        "amount": round(max(0.5, r.lognormvariate(0, 0.6) * cust["avg_amount"]), 2),
        "currency": "USD",
        "country": cust["home_country"] if r.random() < 0.97 else r.choice(COUNTRIES),
        "device_id": r.choice(cust["devices"]),
        "channel": "online" if m["online"] else "pos",
        "is_fraud": 0,
        "fraud_type": None,
    }


def fraud_episode(day, r):
    """One fraud episode: a coherent burst of related fraudulent transactions."""
    cust = customers[r.randrange(N_CUSTOMERS)]
    kind = r.choice(["card_testing", "account_takeover", "merchant_collusion"])
    base = datetime.combine(day, datetime.min.time()) + timedelta(
        hours=r.randint(0, 23), minutes=r.randint(0, 59))
    txns = []

    if kind == "card_testing":
        # burst of tiny online charges, minutes apart, at random online merchants
        online = [m for m in merchants if m["online"]]
        for k in range(r.randint(8, 20)):
            m = r.choice(online)
            txns.append((base + timedelta(minutes=k * r.randint(1, 4)), m,
                         round(r.uniform(0.5, 3.0), 2), cust["home_country"],
                         f"D{uuid.UUID(int=r.getrandbits(128)).hex[:10]}", "online"))
    elif kind == "account_takeover":
        # sudden high-value spend from a foreign country on a new device
        country = r.choice(FOREIGN)
        device = f"D{uuid.UUID(int=r.getrandbits(128)).hex[:10]}"
        for k in range(r.randint(3, 6)):
            m = merchants[r.randrange(N_MERCHANTS)]
            txns.append((base + timedelta(hours=k * r.uniform(0.3, 2)), m,
                         round(cust["avg_amount"] * r.uniform(8, 30), 2),
                         country, device, "online"))
    else:  # merchant_collusion — repeated near-threshold amounts at one merchant
        m = merchants[r.randrange(N_MERCHANTS)]
        for k in range(r.randint(4, 9)):
            txns.append((base + timedelta(hours=k * r.uniform(1, 4)), m,
                         round(r.uniform(480, 499), 2), cust["home_country"],
                         r.choice(cust["devices"]), "pos"))

    return [{
        "transaction_id": str(uuid.UUID(int=r.getrandbits(128))),
        "event_ts": ts.isoformat(),
        "customer_id": cust["customer_id"],
        "merchant_id": m["merchant_id"],
        "merchant_category": m["category"],
        "amount": amount,
        "currency": "USD",
        "country": country,
        "device_id": device,
        "channel": channel,
        "is_fraud": 1,
        "fraud_type": kind,
    } for ts, m, amount, country, device, channel in txns]


def dirty_up(records, r):
    """Inject data-quality problems: duplicates, nulls, malformed amounts."""
    dirty = []
    for rec in records:
        if r.random() < 0.005:                      # exact duplicate
            dirty.append(dict(rec))
        if r.random() < 0.004:                      # null merchant
            rec = {**rec, "merchant_id": None, "merchant_category": None}
        if r.random() < 0.003:                      # negative amount (reversal noise)
            rec = {**rec, "amount": -abs(rec["amount"])}
        if r.random() < 0.003:                      # amount as string
            rec = {**rec, "amount": str(rec["amount"])}
        dirty.append(rec)
    r.shuffle(dirty)
    return dirty

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate one JSONL file per day

# COMMAND ----------

start = date.fromisoformat(dbutils.widgets.get("start_date"))
num_days = int(dbutils.widgets.get("num_days"))
os.makedirs(RAW_DIR, exist_ok=True)

for d in range(num_days):
    day = start + timedelta(days=d)
    path = f"{RAW_DIR}/transactions_{day.isoformat()}.jsonl"
    if os.path.exists(path):
        print(f"skip (exists): {path}")
        continue

    # per-day RNG keyed to the date, so any day is reproducible independently
    r = random.Random(f"{SEED}-{day.isoformat()}")
    records = []
    weekend_factor = 1.25 if day.weekday() >= 5 else 1.0
    for _ in range(int(TXNS_PER_DAY * weekend_factor)):
        records.append(legit_txn(customers[r.randrange(N_CUSTOMERS)], day, r))
    for _ in range(FRAUD_EPISODES_PER_DAY):
        records.extend(fraud_episode(day, r))
    records = dirty_up(records, r)

    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    fraud_n = sum(1 for x in records if x.get("is_fraud") == 1)
    print(f"wrote {path}: {len(records)} rows ({fraud_n} fraud)")

# COMMAND ----------

files = sorted(os.listdir(RAW_DIR))
print(f"{len(files)} files in landing zone, latest: {files[-1] if files else '-'}")
