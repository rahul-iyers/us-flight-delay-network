"""
/airports  — airport-level stats and hourly delay profiles.
"""
import math
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from functools import lru_cache

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import AIRPORT_STATS, HOURLY_DELAYS
from utils import df_to_json_records

router = APIRouter()


@lru_cache(maxsize=1)
def _load_airports() -> pd.DataFrame:
    if not AIRPORT_STATS.exists():
        raise FileNotFoundError(f"airport_stats.parquet not found at {AIRPORT_STATS}. Run the pipeline first.")
    return pd.read_parquet(AIRPORT_STATS)


@lru_cache(maxsize=1)
def _load_hourly() -> pd.DataFrame:
    if not HOURLY_DELAYS.exists():
        raise FileNotFoundError("hourly_delays.parquet not found.")
    return pd.read_parquet(HOURLY_DELAYS)


# ---------------------------------------------------------------------------

@router.get("/")
def list_airports(
    state: Optional[str] = Query(None, description="Filter by 2-letter state code"),
    min_flights: int = Query(0, ge=0, description="Minimum total flights"),
    limit: int = Query(500, ge=1, le=2000),
):
    """Return all airports with their stats (used for the network map)."""
    df = _load_airports()
    if state:
        df = df[df["state"].str.upper() == state.upper()]
    if min_flights:
        df = df[df["total_flights"] >= min_flights]
    df = df.head(limit)
    return df_to_json_records(df)


@router.get("/hourly/all")
def get_all_hourly(
    hour_start: int = Query(0, ge=0, le=23),
    hour_end: int = Query(23, ge=0, le=23),
):
    """
    Return avg dep delay per airport for a given hour range.
    Used by the network map to recolor nodes when the hour slider moves.
    Returns a compact list: [{airport_code, avg_dep_delay, flight_count}]
    """
    df = _load_hourly()
    if hour_start <= hour_end:
        df = df[(df["hour"] >= hour_start) & (df["hour"] <= hour_end)]
    else:
        # wrap-around (e.g. 22-03)
        df = df[(df["hour"] >= hour_start) | (df["hour"] <= hour_end)]
    agg = (
        df.groupby("airport_code")
        .agg(
            avg_dep_delay=("avg_dep_delay", "mean"),
            flight_count=("flight_count", "sum"),
        )
        .reset_index()
    )
    return df_to_json_records(agg)


@router.get("/{code}")
def get_airport(code: str):
    """Return stats for a single airport."""
    df = _load_airports()
    row = df[df["airport_code"] == code.upper()]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Airport '{code}' not found")
    return df_to_json_records(row)[0]


@router.get("/{code}/hourly")
def get_hourly(code: str):
    """Return per-hour delay stats for a single airport."""
    df = _load_hourly()
    rows = df[df["airport_code"] == code.upper()]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"No hourly data for '{code}'")
    return df_to_json_records(rows.sort_values("hour"))
