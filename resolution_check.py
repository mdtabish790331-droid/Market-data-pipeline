# resolution_check.py
import psycopg2

conn = psycopg2.connect(
    dbname="market_data",
    user="postgres",
    password="postgres",
    host="127.0.0.1"
)
cursor = conn.cursor()

# Koi bhi 5 secret codes dekho
cursor.execute("SELECT secret_asset_id, real_asset_id FROM asset_registry LIMIT 5")
codes = cursor.fetchall()

print("🔍 RESOLUTION CHECK")
print("-" * 50)
for secret, real in codes:
    print(f"Secret: {secret} → Real: {real}")
    
    # Is secret code ke kitne records hain?
    cursor.execute("SELECT COUNT(*) FROM market_data_secret WHERE secret_asset_id = %s", (secret,))
    count = cursor.fetchone()[0]
    print(f"   📊 Records: {count}\n")

cursor.close()
conn.close()