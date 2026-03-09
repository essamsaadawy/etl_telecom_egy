
import logging
from src.config import Settings
from src.extract import extract_worldbank_egy
from src.transform import transform
from src.load import load_files, load_sqlite_upsert

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def run():
    s = Settings()
    logging.info("ETL started")

    # EXTRACT
    rows = extract_worldbank_egy(
        base_url=s.WB_BASE_URL,
        country=s.COUNTRY,
        indicator=s.INDICATOR_CODE,
        date_range=s.DATE_RANGE,
        per_page=s.PER_PAGE,
        raw_dir=s.RAW_DIR
    )
    logging.info("Extracted %d rows", len(rows))

    # TRANSFORM
    clean_df, bad_df = transform(rows, s.BAD_DIR)
    logging.info("Transform complete: clean=%d bad=%d", len(clean_df), len(bad_df))

    # LOAD
    load_files(clean_df, s.OUT_DIR)
    load_sqlite_upsert(clean_df, s.SQLITE_PATH, s.TABLE_NAME)
    logging.info("Load complete: CSV/Parquet + SQLite saved")

    logging.info("ETL finished successfully")

if __name__ == "__main__":
    run()
