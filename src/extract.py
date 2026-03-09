
import os
import json
import time
import requests

def extract_worldbank_egy(base_url: str, country: str, indicator: str, date_range: str,
                          per_page: int, raw_dir: str) -> list[dict]:
    """
    Fetch World Bank indicator data for a single country (EGY), handle paging,
    save raw snapshot JSON, return rows list (observations).
    """
    os.makedirs(raw_dir, exist_ok=True)

    endpoint = f"{base_url}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "date": date_range, "per_page": per_page, "page": 1}

    try:
        r = requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
    except requests.RequestException as e:
        raise RuntimeError(f"EXTRACT failed: request error: {e}") from e
    except ValueError as e:
        raise RuntimeError("EXTRACT failed: invalid JSON response") from e

    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError("EXTRACT failed: unexpected API response structure")

    meta = payload[0]
    rows = payload[1] or []
    total_pages = int(meta.get("pages", 1))

    for p in range(2, total_pages + 1):
        params["page"] = p
        r = requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        page_payload = r.json()
        rows.extend(page_payload[1] or [])
        time.sleep(0.05)

    snapshot = {
        "country": country,
        "indicator": indicator,
        "date_range": date_range,
        "meta": meta,
        "rows": rows
    }

    out_path = os.path.join(raw_dir, f"wb_{country}_{indicator}_{date_range.replace(':','-')}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)

    return rows
