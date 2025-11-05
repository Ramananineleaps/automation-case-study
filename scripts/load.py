import pandas as pd
import sqlite3
import os
from datetime import datetime

CLEAN_PATH = "data/articles_clean.csv"
DB_PATH = "data/news_articles.db"
TABLE_NAME = "news_articles"

# -----------------------------------------------------------
# Step 1: Check if cleaned file exists
# -----------------------------------------------------------
if not os.path.exists(CLEAN_PATH):
    print("❌ Error: Cleaned data not found. Run transform.py first.")
    exit()

# -----------------------------------------------------------
# Step 2: Load the cleaned CSV
# -----------------------------------------------------------
df = pd.read_csv(CLEAN_PATH)
print(f"📦 Loading {len(df)} cleaned articles into database...")

# -----------------------------------------------------------
# Step 3: Connect to SQLite database
# -----------------------------------------------------------
os.makedirs("data", exist_ok=True)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# -----------------------------------------------------------
# Step 4: Create table if not exists
# -----------------------------------------------------------
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    author TEXT,
    pub_date TEXT,
    scraped_at TEXT,
    url TEXT
)
""")

# -----------------------------------------------------------
# Step 5: Insert data (append mode)
# -----------------------------------------------------------
df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)

# -----------------------------------------------------------
# Step 6: Verify load
# -----------------------------------------------------------
row_count = cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"✅ Data successfully loaded into {DB_PATH}")
print(f"Total records in table '{TABLE_NAME}': {row_count}")

# -----------------------------------------------------------
# Step 7: Commit and close
# -----------------------------------------------------------
conn.commit()
conn.close()