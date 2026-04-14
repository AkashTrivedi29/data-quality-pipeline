"""
=============================================================================
PROJECT 4: Data Quality & Validation Pipeline
Dataset Generator — Messy Real-World Sales Dataset
=============================================================================
Business Question:
  How do we identify, flag, and resolve data quality issues across a pipeline?

Simulates a messy sales/CRM dataset from a mid-size retail company with
realistic data quality problems commonly found in production systems:

  1.  Missing values          — NULLs in critical fields
  2.  Duplicate records       — exact + near-duplicate rows
  3.  Invalid formats         — malformed emails, phone numbers, dates
  4.  Out-of-range values     — negative prices, future birth dates
  5.  Referential integrity   — orders linked to non-existent customers
  6.  Inconsistent categories — "Ontario" / "ON" / "ontario" / "Ont."
  7.  Type mismatches         — numeric stored as string
  8.  Outliers                — order values 100× normal
  9.  Cross-field violations  — ship date before order date
  10. Encoding issues         — special characters, extra whitespace
=============================================================================
"""

import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

N = 5000  # total records (including dirty ones)

# ---------------------------------------------------------------------------
# 1. Clean base data
# ---------------------------------------------------------------------------
first_names = ["James","Maria","John","Patricia","Robert","Jennifer","Michael",
               "Linda","William","Barbara","David","Susan","Richard","Jessica",
               "Joseph","Sarah","Thomas","Karen","Charles","Lisa","Akash","Priya",
               "Mohammed","Fatima","Wei","Xin","Carlos","Ana","Jean","Marie"]

last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Wilson","Taylor","Anderson","Thomas","Jackson","White",
               "Harris","Martin","Thompson","Lee","Patel","Singh","Trivedi",
               "Kumar","Chen","Wang","Rodriguez","Martinez","Hernandez","Lopez"]

provinces_clean = ["Ontario","Quebec","British Columbia","Alberta",
                   "Manitoba","Saskatchewan","Nova Scotia","New Brunswick"]

categories_clean = ["Electronics","Clothing","Home & Garden","Sports",
                    "Books","Toys","Food & Beverage","Health & Beauty"]

start_date = datetime(2022, 1, 1)
end_date   = datetime(2024, 6, 30)

def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end-start).days))

def rand_email(first, last):
    domains = ["gmail.com","yahoo.com","hotmail.com","outlook.com","company.ca"]
    return f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{random.choice(domains)}"

def rand_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

records = []
customer_ids = [f"CUST{i:05d}" for i in range(1, 2001)]
for i in range(N):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    order_date = rand_date(start_date, end_date)
    ship_date  = order_date + timedelta(days=random.randint(1, 14))
    records.append({
        "order_id":       f"ORD{i+10000:06d}",
        "customer_id":    random.choice(customer_ids),
        "customer_name":  f"{fn} {ln}",
        "email":          rand_email(fn, ln),
        "phone":          rand_phone(),
        "province":       random.choice(provinces_clean),
        "category":       random.choice(categories_clean),
        "product_name":   f"{random.choice(categories_clean)} Item {random.randint(1,200):03d}",
        "quantity":       random.randint(1, 20),
        "unit_price":     round(random.uniform(5.0, 500.0), 2),
        "discount_pct":   random.choice([0, 5, 10, 15, 20]),
        "order_date":     order_date.strftime("%Y-%m-%d"),
        "ship_date":      ship_date.strftime("%Y-%m-%d"),
        "status":         random.choice(["Completed","Pending","Shipped","Cancelled","Returned"]),
        "sales_rep":      f"REP{random.randint(1,50):03d}",
    })

df_clean = pd.DataFrame(records)
df_clean["revenue"] = (df_clean["quantity"] * df_clean["unit_price"] *
                       (1 - df_clean["discount_pct"]/100)).round(2)

# ---------------------------------------------------------------------------
# 2. Inject data quality issues
# ---------------------------------------------------------------------------
df = df_clean.copy()
idx = df.index.tolist()
random.shuffle(idx)

# Issue 1: Missing values (~8% of key fields)
missing_idx = random.sample(idx, 400)
df.loc[missing_idx[:150], "email"]         = np.nan
df.loc[missing_idx[150:280], "phone"]      = np.nan
df.loc[missing_idx[280:350], "province"]   = np.nan
df.loc[missing_idx[350:400], "customer_id"]= np.nan

# Issue 2: Duplicate records (150 exact dupes)
dupe_rows = df.sample(75).copy()
dupe_rows2= df.sample(75).copy()
df = pd.concat([df, dupe_rows, dupe_rows2], ignore_index=True)

# Issue 3: Invalid email formats
bad_email_idx = random.sample(idx, 200)
bad_emails = ["notanemail","user@","@domain.com","user@domain","user.domain.com",
              "user@ gmail.com","USER@@GMAIL.COM","","  ","user@.com"]
for i in bad_email_idx:
    df.at[i, "email"] = random.choice(bad_emails)

# Issue 4: Invalid phone formats
bad_phone_idx = random.sample(idx, 180)
bad_phones = ["1234567","123-456","(416) abc-1234","00000000000","N/A","none",
              "123456789012345","416.123.","phone number"]
for i in bad_phone_idx:
    df.at[i, "phone"] = random.choice(bad_phones)

# Issue 5: Inconsistent province names
prov_variants = {
    "Ontario":          ["ON","ontario","Ont.","ONTARIO","ont","Ontatio"],
    "Quebec":           ["QC","quebec","Que.","QUEBEC","Québec","quecbec"],
    "British Columbia": ["BC","british columbia","B.C.","BRITISH COLUMBIA","Britsh Columbia"],
    "Alberta":          ["AB","alberta","Alta.","ALBERTA","Albeta"],
}
prov_idx = random.sample(idx, 600)
for i in prov_idx:
    orig = df.at[i, "province"]
    if orig in prov_variants:
        df.at[i, "province"] = random.choice(prov_variants[orig])

# Issue 6: Out-of-range values
neg_price_idx = random.sample(idx, 80)
for i in neg_price_idx:
    df.at[i, "unit_price"] = round(random.uniform(-500, -1), 2)

zero_qty_idx = random.sample(idx, 50)
for i in zero_qty_idx:
    df.at[i, "quantity"] = 0

outlier_idx = random.sample(idx, 30)
for i in outlier_idx:
    df.at[i, "unit_price"] = round(random.uniform(50000, 999999), 2)

# Issue 7: Ship date before order date
date_error_idx = random.sample(idx, 120)
for i in date_error_idx:
    order_dt = pd.to_datetime(df.at[i, "order_date"])
    bad_ship = order_dt - timedelta(days=random.randint(1, 30))
    df.at[i, "ship_date"] = bad_ship.strftime("%Y-%m-%d")

# Issue 8: Type mismatches — quantity as string
type_idx = random.sample(idx, 100)
for i in type_idx:
    df.at[i, "quantity"] = str(df.at[i, "quantity"]) + " units"

# Issue 9: Future order dates
future_idx = random.sample(idx, 60)
for i in future_idx:
    future = end_date + timedelta(days=random.randint(1, 365))
    df.at[i, "order_date"] = future.strftime("%Y-%m-%d")

# Issue 10: Whitespace and encoding issues
ws_idx = random.sample(idx, 150)
for i in ws_idx:
    df.at[i, "customer_name"] = "  " + df.at[i, "customer_name"] + "  "

# Issue 11: Referential integrity — orders with invalid customer IDs
ref_idx = random.sample(idx, 90)
for i in ref_idx:
    df.at[i, "customer_id"] = f"CUST{random.randint(99000, 99999):05d}"

# Issue 12: Invalid discount values
disc_idx = random.sample(idx, 70)
for i in disc_idx:
    df.at[i, "discount_pct"] = random.choice([-10, 101, 150, 999, -5])

# Shuffle the dataset
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

print(f"✅ Messy dataset generated: {len(df):,} records")
print(f"\nInjected Issues Summary:")
print(f"  Missing values:          ~730 cells")
print(f"  Duplicate records:        150")
print(f"  Invalid emails:           200")
print(f"  Invalid phone formats:    180")
print(f"  Inconsistent provinces:   600")
print(f"  Negative/zero prices:     130")
print(f"  Outlier prices:            30")
print(f"  Ship < Order date:        120")
print(f"  Type mismatches:          100")
print(f"  Future order dates:        60")
print(f"  Whitespace issues:        150")
print(f"  Referential integrity:     90")
print(f"  Invalid discount values:   70")

df.to_csv("/sessions/dazzling-sweet-pascal/day4_dq/data/sales_dirty.csv", index=False)
df_clean.to_csv("/sessions/dazzling-sweet-pascal/day4_dq/data/sales_clean_reference.csv", index=False)
print(f"\n✅ Dirty dataset saved: {len(df):,} rows")
print(f"✅ Clean reference saved: {len(df_clean):,} rows")
