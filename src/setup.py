import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="wikievents",
    user="dataeng",
    password="dataeng123"
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS wiki_events (
        id              SERIAL PRIMARY KEY,
        event_id        BIGINT UNIQUE,
        event_type      VARCHAR(50),
        title           TEXT,
        wiki            VARCHAR(100),
        user_name       TEXT,
        bot             BOOLEAN DEFAULT FALSE,
        is_minor        BOOLEAN DEFAULT FALSE,
        server_name     VARCHAR(100),
        namespace       INTEGER,
        timestamp       BIGINT,
        comment         TEXT,
        url             TEXT,
        bytes_changed   INTEGER DEFAULT 0,
        raw_event       JSONB,
        ingested_at     TIMESTAMP DEFAULT NOW()
    );
""")

conn.commit()
cursor.close()
conn.close()
print("✅ Table wiki_events created successfully!")
