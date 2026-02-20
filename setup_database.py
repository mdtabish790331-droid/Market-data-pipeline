import psycopg2

conn = psycopg2.connect(
    dbname="market_data",
    user="market_user",
    password="pass123",
    host="127.0.0.1",
    port="5432"
)


cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    asset_hash VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS market_data_real (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER REFERENCES assets(id),
    datetime TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS market_data_public (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER REFERENCES assets(id),
    datetime TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION
);
""")

conn.commit()
cursor.close()
conn.close()

print("✅ PostgreSQL database and tables created successfully")
