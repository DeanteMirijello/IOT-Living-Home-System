# db_manager.py
# Handles local SQLite logging + cloud PostgreSQL sync (NeonDB)

import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

# Path for local database
LOCAL_DB_PATH = Path(__file__).with_name("local.db")

# Cloud DB connection (same as Flask)
DB_CONN = (
    "postgresql://neondb_owner:npg_SdpFWnquG9t6"
    "@ep-jolly-snow-a4ymm7wa-pooler.us-east-1.aws.neon.tech/neondb"
    "?sslmode=require"
)

# -----------------------------------------------------
# LOCAL DATABASE
# -----------------------------------------------------

def init_local_db():
    """Create SQLite local database if not exists."""
    conn = sqlite3.connect(LOCAL_DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor TEXT NOT NULL,
            value REAL NOT NULL,
            timestamp TEXT NOT NULL
        );
    """)

    conn.commit()
    conn.close()
    print("[LOCAL-DB] Initialized local.db")

def insert_local(sensor, value, timestamp):
    """Insert a row into local SQLite database."""
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO sensor_data (sensor, value, timestamp) VALUES (?, ?, ?)",
            (sensor, value, timestamp)
        )

        conn.commit()
        conn.close()
        print(f"[LOCAL-DB] Stored locally: {sensor}={value} at {timestamp}")

    except Exception as e:
        print(f"[LOCAL-DB] ERROR inserting locally: {e}")

# -----------------------------------------------------
# CLOUD DATABASE
# -----------------------------------------------------

def insert_cloud(sensor, value, timestamp):
    """Insert data directly into NeonDB (cloud)."""
    try:
        conn = psycopg2.connect(DB_CONN, cursor_factory=RealDictCursor)
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO sensor_data (sensor, value, timestamp)
            VALUES (%s, %s, %s);
        """, (sensor, value, timestamp))

        conn.commit()
        cur.close()
        conn.close()

        print(f"[CLOUD] Inserted: {sensor}={value} at {timestamp}")

    except Exception as e:
        print(f"[CLOUD] ERROR inserting to cloud: {e}")
        raise e   # Pi code will fall back to insert_local()

# -----------------------------------------------------
# SYNC LOGIC
# -----------------------------------------------------

def sync_local_to_cloud():
    """
    Push all unsynced SQLite rows to NeonDB.
    After successful upload, delete them locally.
    """
    try:
        # Open local DB
        conn_local = sqlite3.connect(LOCAL_DB_PATH)
        cur_local = conn_local.cursor()

        cur_local.execute("SELECT id, sensor, value, timestamp FROM sensor_data ORDER BY id ASC")
        rows = cur_local.fetchall()

        if len(rows) == 0:
            conn_local.close()
            return  # nothing to sync

        print(f"[SYNC] Attempting to sync {len(rows)} entries...")

        # Connect to cloud
        conn_cloud = psycopg2.connect(DB_CONN, cursor_factory=RealDictCursor)
        cur_cloud = conn_cloud.cursor()

        # Try uploading each row
        uploaded_ids = []

        for row in rows:
            row_id, sensor, value, timestamp = row

            try:
                cur_cloud.execute("""
                    INSERT INTO sensor_data (sensor, value, timestamp)
                    VALUES (%s, %s, %s);
                """, (sensor, value, timestamp))
                uploaded_ids.append(row_id)

            except Exception as e:
                print(f"[SYNC] Failed to upload row {row_id}: {e}")
                # Stop sync attempt for now — keep remaining rows in SQLite
                break

        conn_cloud.commit()
        cur_cloud.close()
        conn_cloud.close()

        # Delete only uploaded rows (not failed ones)
        if uploaded_ids:
            cur_local.executemany(
                "DELETE FROM sensor_data WHERE id = ?",
                [(i,) for i in uploaded_ids]
            )
            conn_local.commit()
            print(f"[SYNC] Successfully synced {len(uploaded_ids)} rows.")

        conn_local.close()

    except Exception as e:
        print(f"[SYNC] ERROR: {e}")
