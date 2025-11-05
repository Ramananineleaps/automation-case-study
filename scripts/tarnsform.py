import pandas as pd
import os
from datetime import datetime
from dateutil import parser

# -----------------------------------------------------------
# 🧭 Step 1: Setup paths
# -----------------------------------------------------------
RAW_PATH = "data/articles_raw.csv"
CLEAN_PATH = "data/articles_clean.csv"

# -----------------------------------------------------------
# 🧹 Step 2: Helper function for date parsing
# -----------------------------------------------------------
def parse_date_safe(x):
    """Try to parse multiple date formats safely into YYYY-MM-DD."""
    if pd.isna(x) or str(x).strip() == "":
        return None
    try:
        # Try fast pandas conversion first
        dt = pd.to_datetime(x, errors="coerce")
        if pd.notna(dt):
            return dt.date()
    except Exception:
        pass

    # Fallback: flexible dateutil parser (handles text formats)
    try:
        return parser.parse(str(x)).date()
    except Exception:
        return None

# -----------------------------------------------------------
# 🧩 Step 3: Load raw data
# -----------------------------------------------------------
if not os.path.exists(RAW_PATH):
    print("❌ Error: Raw data file not found. Run extraction first.")
    exit()

df = pd.read_csv(RAW_PATH)
print(f"\n🧹 Starting transformation: {len(df)} raw rows")

# -----------------------------------------------------------
# 🧼 Step 4: Clean column headers
# -----------------------------------------------------------
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# -----------------------------------------------------------
# 📅 Step 5: Normalize publication dates
# -----------------------------------------------------------
df["pub_date"] = df["pub_date"].apply(parse_date_safe)

# Fill missing dates with scraped_at date (only date portion)
df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
df["pub_date"] = df["pub_date"].fillna(df["scraped_at"].dt.date)

# Convert all dates to string (ISO format)
df["pub_date"] = df["pub_date"].astype(str)
df["scraped_at"] = df["scraped_at"].astype(str)

# -----------------------------------------------------------
# 🧩 Step 6: Deduplicate
# -----------------------------------------------------------
raw_count = len(df)
df = df.drop_duplicates(subset=["title", "pub_date"])
final_count = len(df)

# -----------------------------------------------------------
# 💾 Step 7: Save cleaned file
# -----------------------------------------------------------
os.makedirs("data", exist_ok=True)
df.to_csv(CLEAN_PATH, index=False, encoding="utf-8")

# -----------------------------------------------------------
# 📊 Step 8: Log summary
# -----------------------------------------------------------
print(f"✅ Cleaned data saved to {CLEAN_PATH}")
print(f"Raw: {raw_count}, Duplicates removed: {raw_count - final_count}, Final: {final_count}")

# Optional: display sample
print("\nSample cleaned data:")
print(df.head(5).to_string(index=False))