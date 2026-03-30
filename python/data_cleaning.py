import pandas as pd
import psycopg2
import os

# =========================
# STEP 1: Load Data (FIXED PATH)
# =========================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

raw_file_path = os.path.join(base_dir, "data", "raw_data.csv")
clean_file_path = os.path.join(base_dir, "data", "cleaned_data.csv")

df = pd.read_csv(os.path.join(base_dir, "data", "big_data.csv"))

print("Raw Data:")
print(df.head())

# =========================
# STEP 2: Data Cleaning
# =========================
df.dropna(inplace=True)
df.drop_duplicates(inplace=True)

df['order_date'] = pd.to_datetime(df['order_date'])

# Save cleaned data
df.to_csv(clean_file_path, index=False)

print("\n✅ Cleaned Data Saved!")

# =========================
# STEP 3: Connect to PostgreSQL
# =========================
conn = psycopg2.connect(
    dbname="ricap_db",
    user="postgres",
    password="YASH",   # ⚠️ apna password sahi daalna
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# =========================
# STEP 4: Insert Data (FAST & SAFE)
# =========================
insert_query = """
INSERT INTO orders (customer_id, order_id, order_date, product_id, revenue, quantity)
VALUES (%s, %s, %s, %s, %s, %s)
"""

data = [
    (
        row['customer_id'],
        int(row['order_id']),
        row['order_date'],
        row['product_id'],
        float(row['revenue']),
        int(row['quantity'])
    )
    for _, row in df.iterrows()
]

cursor.executemany(insert_query, data)

# =========================
# STEP 5: Commit & Close
# =========================
conn.commit()
cursor.close()
conn.close()

print("✅ Data inserted into PostgreSQL!")