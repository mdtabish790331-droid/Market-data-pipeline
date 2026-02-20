import psycopg2
import yfinance as yf
from datetime import datetime

conn = psycopg2.connect(
    dbname="market_data",
    user="market_user",
    password="pass123",
    host="127.0.0.1",
    port="5432"
)

cursor = conn.cursor()

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

    cursor.execute("SELECT id FROM assets WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()

    if result:
        asset_id = result[0]
    else:
        cursor.execute(
            "INSERT INTO assets (symbol, asset_hash, created_at) VALUES (%s, %s, %s) RETURNING id",
            (symbol, symbol + "_hash", datetime.now())
        )
        asset_id = cursor.fetchone()[0]

    conn.commit()

    data = yf.download(symbol, period="5y", interval="1d", progress=False)

    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO market_data_real 
            (asset_id, datetime, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asset_id, datetime) DO NOTHING
        """, (
            asset_id,
            index,
            float(row["Open"]),
            float(row["High"]),
            float(row["Low"]),
            float(row["Close"]),
            float(row["Volume"])
        ))

    conn.commit()

cursor.close()
conn.close()

print("✅ NIFTY 50 data inserted successfully")
