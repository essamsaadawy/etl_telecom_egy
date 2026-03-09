
import os
import sqlite3
import pandas as pd

def load_files(df: pd.DataFrame, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, "telecom_clean.csv"), index=False)
    df.to_parquet(os.path.join(out_dir, "telecom.parquet"), index=False)

def load_sqlite_upsert(df: pd.DataFrame, sqlite_path: str, table_name: str) -> None:
    """
    Idempotent load: PRIMARY KEY + INSERT OR REPLACE (rerun won't duplicate).
    """
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                iso3 TEXT NOT NULL,
                year INTEGER NOT NULL,
                indicator_code TEXT NOT NULL,
                country_name TEXT,
                indicator_name TEXT,
                mobile_subs_per_100 REAL,
                yoy_change REAL,
                penetration_band TEXT,
                PRIMARY KEY (iso3, year, indicator_code)
            )
        """)

        rows = df.to_dict(orient="records")
        cur.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name}
            (iso3, year, indicator_code, country_name, indicator_name,
             mobile_subs_per_100, yoy_change, penetration_band)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r.get("iso3"),
                    int(r.get("year")),
                    r.get("indicator_code"),
                    r.get("country_name"),
                    r.get("indicator_name"),
                    r.get("mobile_subs_per_100"),
                    r.get("yoy_change"),
                    r.get("penetration_band"),
                )
                for r in rows
            ]
        )

        conn.commit()
    finally:
        conn.close()
