
from src.transform import transform

def test_transform(tmp_path):
    rows = [
        {
            "countryiso3code": "EGY",
            "country": {"value": "Egypt, Arab Rep."},
            "date": "2021",
            "value": 90.0,
            "indicator": {"id": "IT.CEL.SETS.P2", "value": "Mobile cellular subscriptions (per 100 people)"}
        },
        {
            "countryiso3code": "EGY",
            "country": {"value": "Egypt, Arab Rep."},
            "date": "2022",
            "value": 93.0,
            "indicator": {"id": "IT.CEL.SETS.P2", "value": "Mobile cellular subscriptions (per 100 people)"}
        },
        # ✅ bad row with DIFFERENT year (not duplicate key)
        {
            "countryiso3code": "EGY",
            "country": {"value": "Egypt, Arab Rep."},
            "date": "2023",
            "value": -5,  # invalid
            "indicator": {"id": "IT.CEL.SETS.P2", "value": "Mobile cellular subscriptions (per 100 people)"}
        },
    ]

    clean, bad = transform(rows, bad_dir=str(tmp_path))
    assert "yoy_change" in clean.columns
    assert "penetration_band" in clean.columns
    assert len(bad) == 1