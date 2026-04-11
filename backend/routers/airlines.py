"""
/airlines  — airline-level stats and monthly trends.
"""
from functools import lru_cache
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import AIRLINE_STATS, MONTHLY_STATS
from utils import df_to_json_records

router = APIRouter()

AIRLINE_NAMES: dict[str, str] = {
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


@lru_cache(maxsize=1)
def _load_airlines() -> pd.DataFrame:
    if not AIRLINE_STATS.exists():
        raise FileNotFoundError("airline_stats.parquet not found. Run the pipeline first.")
    df = pd.read_parquet(AIRLINE_STATS)
    # Prefer the embedded full name from the CSV, fall back to lookup table
    if "airline_full_name" in df.columns:
        df["airline_name"] = df["airline_full_name"].where(
            df["airline_full_name"].notna(),
            df["airline_code"].map(AIRLINE_NAMES).fillna(df["airline_code"])
        )
    else:
        df["airline_name"] = df["airline_code"].map(AIRLINE_NAMES).fillna(df["airline_code"])
    return df


@lru_cache(maxsize=1)
def _load_monthly() -> pd.DataFrame:
    if not MONTHLY_STATS.exists():
        return pd.DataFrame()
    return pd.read_parquet(MONTHLY_STATS)


@router.get("/")
def list_airlines(limit: int = Query(50, ge=1, le=200)):
    return df_to_json_records(_load_airlines().head(limit))


@router.get("/monthly")
def get_monthly_trends():
    df = _load_monthly()
    if df.empty:
        return []
    return df_to_json_records(df)


@router.get("/{code}")
def get_airline(code: str):
    df = _load_airlines()
    row = df[df["airline_code"] == code.upper()]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Airline '{code}' not found")
    return df_to_json_records(row)[0]
