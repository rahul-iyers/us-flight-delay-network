# /airlines: airline stats and monthly trends
import pandas as pd
from fastapi import APIRouter

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import df_to_json_records

ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

AIRLINE_STATS = PROCESSED_DIR / "airline_stats.parquet"

router = APIRouter()

AIRLINE_NAMES = {
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue Airways",
    "AS": "Alaska Airlines",
    "NK": "Spirit Airlines",
    "F9": "Frontier Airlines",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines",
    "SY": "Sun Country Airlines",
    "9E": "Endeavor Air",
    "MQ": "Envoy Air",
    "OH": "PSA Airlines",
    "OO": "SkyWest Airlines",
    "YX": "Republic Airways",
    "YV": "Mesa Air",
    "CP": "Compass Airlines",
    "PT": "Piedmont Airlines",
    "EV": "ExpressJet",
    "QX": "Horizon Air",
}


def _load_airlines() -> pd.DataFrame:
    df = pd.read_parquet(AIRLINE_STATS)
    # try to use embedded full name from csv, fall back to lookup table
    if "airline_full_name" in df.columns:
        df["airline_name"] = df["airline_full_name"].where(
            df["airline_full_name"].notna(),
            df["airline_code"].map(AIRLINE_NAMES).fillna(df["airline_code"])
        )
    else:
        df["airline_name"] = df["airline_code"].map(AIRLINE_NAMES).fillna(df["airline_code"])
    return df

@router.get("/")
def list_airlines(limit: int = 50):
    return df_to_json_records(_load_airlines().head(limit))

@router.get("/{code}")
def get_airline(code: str):
    df = _load_airlines()
    row = df[df["airline_code"] == code.upper()]
    return df_to_json_records(row)[0]
