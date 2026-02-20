import psycopg2
import yfinance as yf
from datetime import datetime

# ✅ FIX 1: Sahi database credentials
conn = psycopg2.connect(
    dbname="market_data",
    user="postgres",           # postgres user use karein
    password="postgres",        # apna password daalein
    host="127.0.0.1",
    port="5432"
)

cursor = conn.cursor()

# ✅ FIX 2: Pehle check karein ki assets table exist karta hai ya nahi
cursor.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(50) UNIQUE NOT NULL,
        asset_hash VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

nifty_50 = [
    "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS",
    "SBIN.NS","BHARTIARTL.NS","ITC.NS","LT.NS","HCLTECH.NS",
    "AXISBANK.NS","KOTAKBANK.NS","HINDUNILVR.NS","ASIANPAINT.NS","MARUTI.NS",
    "SUNPHARMA.NS","TITAN.NS","ULTRACEMCO.NS","BAJFINANCE.NS","WIPRO.NS",
    "NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","M&M.NS",
    "TATAMOTORS.NS","ADANIENT.NS","ADANIPORTS.NS","BAJAJFINSV.NS","COALINDIA.NS",
    "INDUSINDBK.NS","TECHM.NS","JSWSTEEL.NS","HDFCLIFE.NS","DIVISLAB.NS",
    "DRREDDY.NS","GRASIM.NS","EICHERMOT.NS","BRITANNIA.NS","CIPLA.NS",
    "HEROMOTOCO.NS","SBILIFE.NS","APOLLOHOSP.NS","UPL.NS","BPCL.NS",
    "SHREECEM.NS","TATACONSUM.NS","HINDALCO.NS","BAJAJ-AUTO.NS","TRENT.NS"
]

for symbol in nifty_50:
    print(f"Downloading {symbol}...")
    
    # ✅ Asset check karein
    cursor.execute("SELECT id FROM assets WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()
    
    if result:
        asset_id = result[0]
        print(f"  → Asset exists: ID {asset_id}")
    else:
        cursor.execute(
            "INSERT INTO assets (symbol, asset_hash, created_at) VALUES (%s, %s, %s) RETURNING id",
            (symbol, symbol + "_hash", datetime.now())
        )
        asset_id = cursor.fetchone()[0]
        print(f"  → New asset created: ID {asset_id}")
    
    conn.commit()
    
    # ✅ Data download karein
    data = yf.download(symbol, period="5y", interval="1d", progress=False)
    print(f"  → Downloaded {len(data)} records")
    
    count = 0
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO market_data_real 
                (asset_id, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (asset_id, datetime) DO NOTHING
            """, (
                asset_id,
                index,  # datetime
                float(row["Open"].iloc[0]) if hasattr(row["Open"], 'iloc') else float(row["Open"]),
                float(row["High"].iloc[0]) if hasattr(row["High"], 'iloc') else float(row["High"]),
                float(row["Low"].iloc[0]) if hasattr(row["Low"], 'iloc') else float(row["Low"]),
                float(row["Close"].iloc[0]) if hasattr(row["Close"], 'iloc') else float(row["Close"]),
                int(row["Volume"].iloc[0]) if hasattr(row["Volume"], 'iloc') else int(row["Volume"])
            ))
            count += 1
        except Exception as e:
            print(f"  → Error inserting record: {e}")
    
    conn.commit()
    print(f"  → Inserted {count} records for {symbol}")

cursor.close()
conn.close()
print("✅ NIFTY 50 data inserted successfully")