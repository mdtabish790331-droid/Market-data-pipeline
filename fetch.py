import yfinance as yf
import psycopg2
from datetime import datetime
import time
import hashlib
import pandas as pd

DB_CONFIG = {
    "host": "127.0.0.1",
    "database": "market_data",
    "user": "postgres",
    "password": "pass123"
}

STOCKS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS",
    "ITC.NS", "HINDUNILVR.NS", "LT.NS", "SBIN.NS", "BHARTIARTL.NS",
    "KOTAKBANK.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS",
    "HCLTECH.NS", "BAJFINANCE.NS", "WIPRO.NS", "SUNPHARMA.NS",
    "TITAN.NS", "ULTRACEMCO.NS", "NESTLEIND.NS", "ONGC.NS", "NTPC.NS",
    "POWERGRID.NS", "JSWSTEEL.NS", "TATAMOTORS.NS", "M&M.NS",
    "BAJAJFINSV.NS", "ADANIENT.NS", "ADANIPORTS.NS", "INDUSINDBK.NS",
    "TECHM.NS", "DRREDDY.NS", "CIPLA.NS", "EICHERMOT.NS",
    "GRASIM.NS", "HEROMOTOCO.NS", "BRITANNIA.NS", "APOLLOHOSP.NS",
    "COALINDIA.NS", "DIVISLAB.NS", "SBILIFE.NS", "HDFCLIFE.NS",
    "TATASTEEL.NS", "UPL.NS", "BAJAJ-AUTO.NS", "SHREECEM.NS",
    "BPCL.NS", "HINDALCO.NS", "IOC.NS"
]

# ================= FETCH DATA =================
def fetch_data(symbol):
    try:
        print(f"\n📥 Downloading {symbol}...")

        df = yf.download(
            symbol,
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False
        )

        if df.empty:
            print("❌ No data found")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.reset_index(inplace=True)
        df.rename(columns={"Date": "datetime"}, inplace=True)

        print(f"✅ Rows fetched: {len(df)}")
        return df

    except Exception as e:
        print("Fetch Error:", e)
        return None
def get_asset_id_from_hash(cursor, asset_hash):
    cursor.execute("""
        SELECT id FROM asset_registry
        WHERE asset_hash = %s
    """, (asset_hash,))
    
    result = cursor.fetchone()
    return result[0] if result else None


def get_market_data(cursor, asset_hash):
    asset_id = get_asset_id_from_hash(cursor, asset_hash)
    
    if not asset_id:
        print("Invalid Asset Hash")
        return
    
    cursor.execute("""
        SELECT datetime, open, high, low, close, volume
        FROM market_data_real
        WHERE asset_id = %s
        ORDER BY datetime DESC
    """, (asset_id,))
    
    return cursor.fetchall()

# ================= SAVE TO DATABASE =================
def save_to_db(df, symbol):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 🔹 Create asset hash
        asset_hash = hashlib.sha256(symbol.encode()).hexdigest()

        # 1️⃣ Insert into asset_registry (NOT assets)
        cursor.execute("""
            INSERT INTO asset_registry (symbol, asset_hash, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING;
        """, (symbol, asset_hash, datetime.now()))

        conn.commit()

        # 2️⃣ Get asset_id from asset_registry
        cursor.execute(
            "SELECT id FROM asset_registry WHERE symbol = %s;",
            (symbol,)
        )

        result = cursor.fetchone()

        if result is None:
            print("❌ Asset ID not found")
            conn.close()
            return

        asset_id = result[0]

        # 3️⃣ Prepare bulk rows
        rows = []
        for row in df.itertuples(index=False):
            rows.append((
                asset_id,
                row.datetime,
                float(row.Open),
                float(row.High),
                float(row.Low),
                float(row.Close),
                int(row.Volume)
            ))

        print(f"Total rows to insert: {len(rows)}")

        # 4️⃣ Insert into market_data_real
        cursor.executemany("""
            INSERT INTO market_data_real
            (asset_id, datetime, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (asset_id, datetime) DO NOTHING;
        """, rows)

        conn.commit()

        cursor.close()
        conn.close()

        print(f"💾 {symbol} saved successfully!")

    except Exception as e:
        print("Database Error:", e)


# ================= MAIN =================
def main():
    print("🚀 Starting Market Data Ingestion...\n")

    for stock in STOCKS:
        df = fetch_data(stock)
        if df is not None:
            save_to_db(df, stock)
            time.sleep(1)

    print("\n🎯 All Done!")


if __name__ == "__main__":
    main()
