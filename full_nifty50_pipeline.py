# full_nifty50_pipeline.py
import psycopg2
import yfinance as yf
import hashlib
import secrets
from datetime import datetime

# ===========================================
# SECRET CODE BANANE KA FUNCTION
# ===========================================
def create_secret_code(real_name):
    """
    REAL NAME se SECRET CODE banayein
    Example: "RELIANCE.NS" → "7d8f3a2b1c9e5f4a"
    """
    salt = secrets.token_hex(8)
    text_to_hash = f"{real_name}_{salt}"
    hash_obj = hashlib.sha256(text_to_hash.encode())
    secret_code = hash_obj.hexdigest()[:16]
    return secret_code, salt

# ===========================================
# DATABASE CONNECTION
# ===========================================
print("Connecting to database...")
conn = psycopg2.connect(
    dbname="market_data",
    user="postgres",
    password="postgres",  # ✅ APNA PASSWORD YAHAN DALO
    host="127.0.0.1"
)
cursor = conn.cursor()
print("✅ Connected successfully!")

# ===========================================
# NIFTY 50 ALL COMPANIES - POORI LIST
# ===========================================
nifty_50_full = [
    # Bada list - 50 companies
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "LT.NS", "HCLTECH.NS",
    "AXISBANK.NS", "KOTAKBANK.NS", "HINDUNILVR.NS", "ASIANPAINT.NS", "MARUTI.NS",
    "SUNPHARMA.NS", "TITAN.NS", "ULTRACEMCO.NS", "BAJFINANCE.NS", "WIPRO.NS",
    "NESTLEIND.NS", "POWERGRID.NS", "NTPC.NS", "ONGC.NS", "M&M.NS",
    "TATAMOTORS.NS", "ADANIENT.NS", "ADANIPORTS.NS", "BAJAJFINSV.NS", "COALINDIA.NS",
    "INDUSINDBK.NS", "TECHM.NS", "JSWSTEEL.NS", "HDFCLIFE.NS", "DIVISLAB.NS",
    "DRREDDY.NS", "GRASIM.NS", "EICHERMOT.NS", "BRITANNIA.NS", "CIPLA.NS",
    "HEROMOTOCO.NS", "SBILIFE.NS", "APOLLOHOSP.NS", "UPL.NS", "BPCL.NS",
    "SHREECEM.NS", "TATACONSUM.NS", "HINDALCO.NS", "BAJAJ-AUTO.NS", "TRENT.NS"
]

print(f"\n{'='*60}")
print(f"🚀 STARTING PIPELINE - {len(nifty_50_full)} NIFTY 50 COMPANIES")
print(f"{'='*60}\n")

# ===========================================
# MAIN LOOP - SAB COMPANIES KE LIYE
# ===========================================
success_count = 0
failed_count = 0

for index, symbol in enumerate(nifty_50_full, 1):
    print(f"\n📊 [{index}/50] Processing: {symbol}")
    print("-" * 40)
    
    try:
        # STEP 1: Secret code banao
        secret_code, salt = create_secret_code(symbol)
        print(f"   🔐 Secret Code: {secret_code}")
        
        # STEP 2: Registry mein store karo
        cursor.execute("""
            INSERT INTO asset_registry (real_asset_id, secret_asset_id, salt, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (real_asset_id) DO UPDATE SET
                secret_asset_id = EXCLUDED.secret_asset_id,
                salt = EXCLUDED.salt
            RETURNING id
        """, (symbol, secret_code, salt, datetime.now()))
        
        registry_id = cursor.fetchone()[0]
        print(f"   📝 Registry ID: {registry_id}")
        
        # STEP 3: yfinance se data download karo
        print(f"   ⬇️ Downloading data...")
        data = yf.download(symbol, period="1y", interval="1d", progress=False)
        
        if data.empty:
            print(f"   ⚠️ No data found for {symbol}")
            failed_count += 1
            continue
            
        print(f"   📈 Downloaded {len(data)} trading days")
        
        # STEP 4: Data insert karo secret code ke saath
        records_inserted = 0
        for idx, row in data.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO market_data_secret 
                    (secret_asset_id, datetime, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (secret_asset_id, datetime) DO NOTHING
                """, (
                    secret_code,
                    idx,
                    float(row["Open"].iloc[0]) if hasattr(row["Open"], 'iloc') else float(row["Open"]),
                    float(row["High"].iloc[0]) if hasattr(row["High"], 'iloc') else float(row["High"]),
                    float(row["Low"].iloc[0]) if hasattr(row["Low"], 'iloc') else float(row["Low"]),
                    float(row["Close"].iloc[0]) if hasattr(row["Close"], 'iloc') else float(row["Close"]),
                    int(row["Volume"].iloc[0]) if hasattr(row["Volume"], 'iloc') else int(row["Volume"])
                ))
                records_inserted += 1
                
            except Exception as e:
                print(f"      ❌ Error inserting record: {e}")
        
        # STEP 5: Commit karo
        conn.commit()
        success_count += 1
        print(f"   ✅ {records_inserted} records stored for {symbol}")
        print(f"   🔑 Secret Code: {secret_code}")
        
    except Exception as e:
        print(f"   ❌ Error processing {symbol}: {e}")
        failed_count += 1
        conn.rollback()

# ===========================================
# FINAL SUMMARY
# ===========================================
print(f"\n{'='*60}")
print(f"🎉 PIPELINE COMPLETE!")
print(f"{'='*60}")
print(f"✅ Successful: {success_count} companies")
print(f"❌ Failed: {failed_count} companies")
print(f"📊 Total companies processed: {len(nifty_50_full)}")

cursor.close()
conn.close()
print(f"\n💾 All data saved in PostgreSQL database!")
print(f"📁 Tables: asset_registry and market_data_secret")