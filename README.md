

# Telecom ETL Pipeline (EGY) — World Bank Indicators API

## Overview
This repository contains a simple but complete ETL pipeline that programmatically extracts telecom indicator data for Egypt (EGY) from the World Bank Indicators API, transforms it (cleaning + enrichment + validation), and loads the results into CSV/Parquet files and a local SQLite database.  
The goal is to demonstrate end-to-end ETL concepts with clear stage separation, data quality rules, error handling, and reproducible local execution.

**Indicator used:** `IT.CEL.SETS.P2` — Mobile cellular subscriptions (per 100 people).

---

## Data Source
The pipeline uses the World Bank Indicators API and requests data in JSON format.  
Example API call (EGY, 2000–2024):
`https://api.worldbank.org/v2/country/EGY/indicator/IT.CEL.SETS.P2?format=json&date=2000:2024&per_page=20000`

---

## Architecture
Extract  ->  Transform  ->  Load
API JSON -> Clean/Enrich/Validate -> CSV + Parquet + SQLite (idempotent upsert)


### Extract
- Fetches the dataset programmatically from the World Bank API using HTTP requests.
- Handles pagination if multiple pages exist.
- Stores a raw JSON snapshot under `data/raw/` for traceability.

### Transform
The transform stage implements the required “meaningful transformations”:
- **Cleaning**
  - Standardizes column names.
  - Converts fields to correct types (year/value).
  - Removes duplicates using a stable business key.
- **Enrichment / Derived fields**
  - `yoy_change`: year-over-year change in `mobile_subs_per_100`.
  - `penetration_band`: `Low / Medium / High` bucket based on the metric value.
- **Validation (Data Quality Rules)**
  - `iso3` must be exactly 3 characters.
  - `year` must be within a reasonable range (e.g., 1960–2100).
  - `mobile_subs_per_100` must be non-null, >= 0, and within an upper sanity bound.
- Rejected rows are written to `data/bad_records/bad_rows.csv` with a `reject_reason`.

### Load
- Writes clean data outputs to:
  - `data/output/telecom_clean.csv`
  - `data/output/telecom.parquet`
- Loads clean data into SQLite (default: `data/output/telecom.db`)
- **Idempotency:** SQLite uses a composite PRIMARY KEY and `INSERT OR REPLACE` so re-running the pipeline does not create duplicates.

---

## Project Structure

etl_telecom_egy/
src/
config.py
extract.py
transform.py
load.py
main.py
init.py
tests/
test_transform.py
data/
raw/
output/
bad_records/
requirements.txt
README.md
.gitignore



Outputs
After running, you should see:

data/raw/wb_EGY_IT.CEL.SETS.P2_2000-2024.json

data/bad_records/bad_rows.csv

data/output/telecom_clean.csv

data/output/telecom.parquet

data/output/telecom.db
