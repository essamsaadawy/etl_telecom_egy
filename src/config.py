
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    # World Bank API base
    WB_BASE_URL: str = os.getenv("WB_BASE_URL", "https://api.worldbank.org/v2")

    # Telecom indicator: Mobile cellular subscriptions (per 100 people)
    INDICATOR_CODE: str = os.getenv("INDICATOR_CODE", "IT.CEL.SETS.P2")

    # Only Egypt
    COUNTRY: str = os.getenv("COUNTRY", "EGY")

    # Date range supported by World Bank API, using YYYY:YYYY
    DATE_RANGE: str = os.getenv("DATE_RANGE", "2000:2024")

    # Paging
    PER_PAGE: int = int(os.getenv("PER_PAGE", "20000"))

    RAW_DIR: str = "data/raw"
    OUT_DIR: str = "data/output"
    BAD_DIR: str = "data/bad_records"

    SQLITE_PATH: str = os.getenv("SQLITE_PATH", "data/output/telecom.db")
    TABLE_NAME: str = "telecom_mobile_subs"
