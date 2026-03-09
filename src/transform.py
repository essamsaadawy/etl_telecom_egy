
import os
import pandas as pd

def transform(rows: list[dict], bad_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean + enrich + validate.
    Returns clean_df, bad_df and writes bad_df to data/bad_records/bad_rows.csv
    """
    os.makedirs(bad_dir, exist_ok=True)

    df = pd.json_normalize(rows)

    # Keep key fields from WB response
    keep = ["countryiso3code", "country.value", "date", "value", "indicator.id", "indicator.value"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Rename to clean schema
    df = df.rename(columns={
        "countryiso3code": "iso3",
        "country.value": "country_name",
        "date": "year",
        "value": "mobile_subs_per_100",
        "indicator.id": "indicator_code",
        "indicator.value": "indicator_name"
    })

    # Clean types
    df["iso3"] = df["iso3"].astype(str).str.strip()
    df["country_name"] = df["country_name"].astype(str).str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["mobile_subs_per_100"] = pd.to_numeric(df["mobile_subs_per_100"], errors="coerce")

    # Remove duplicates (important for quality & reruns)
    df = df.drop_duplicates(subset=["iso3", "year", "indicator_code"], keep="first")

    # ---- Derive fields (enrichment) ----
    df = df.sort_values(["iso3", "year"])
    df["yoy_change"] = df.groupby("iso3")["mobile_subs_per_100"].diff(1)

    def band(x):
        if pd.isna(x):
            return "Unknown"
        if x < 50:
            return "Low"
        if x < 100:
            return "Medium"
        return "High"

    df["penetration_band"] = df["mobile_subs_per_100"].apply(band)

    # ---- Validation rules ----
    def reject_reason(r):
        if not isinstance(r["iso3"], str) or len(r["iso3"]) != 3:
            return "Invalid ISO3"
        if pd.isna(r["year"]) or r["year"] < 1960 or r["year"] > 2100:
            return "Invalid year"
        if pd.isna(r["mobile_subs_per_100"]):
            return "Missing value"
        if r["mobile_subs_per_100"] < 0 or r["mobile_subs_per_100"] > 400:
            return "Out of expected range"
        return None

    df["reject_reason"] = df.apply(reject_reason, axis=1)

    bad_df = df[df["reject_reason"].notna()].copy()
    clean_df = df[df["reject_reason"].isna()].copy().drop(columns=["reject_reason"])

    bad_df.to_csv(os.path.join(bad_dir, "bad_rows.csv"), index=False)

    return clean_df, bad_df
