# airports: airport stats and hourly delay trends
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import df_to_json_records

ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

AIRPORT_STATS = PROCESSED_DIR / "airport_stats.parquet"
HOURLY_DELAYS = PROCESSED_DIR / "hourly_delays.parquet"
NETWORK_NODES = PROCESSED_DIR / "network_nodes.parquet"

router = APIRouter()


def _load_airports() -> pd.DataFrame:
    df = pd.read_parquet(AIRPORT_STATS)
    centrality_cols = ["airport_code", "community_id", "degree_centrality", "betweenness_centrality", "pagerank"]
    nn = pd.read_parquet(NETWORK_NODES, columns=centrality_cols)
    df = df.merge(nn, on="airport_code", how="left")
    return df


# TODO: delete this later bc it no longer is being used
@router.get("/")
def list_airports(state: Optional[str]=None, min_flights: int=0, limit: int=500):
    df = _load_airports()
    if state:
        df = df[df["state"].str.upper() == state.upper()]
    if min_flights:
        df = df[df["total_flights"] >= min_flights]
    df = df.head(limit)
    return df_to_json_records(df)

@router.get("/hourly/all")
def get_all_hourly(hour_start: int=0, hour_end: int=23):
    df = pd.read_parquet(HOURLY_DELAYS)
    if hour_start <= hour_end:
        df = df[(df["hour"] >= hour_start) & (df["hour"] <= hour_end)]
    else:
        # wraparound for time (like 22-03)
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
    df = _load_airports()
    row = df[df["airport_code"] == code.upper()]
    return df_to_json_records(row)[0]

@router.get("/{code}/hourly")
def get_hourly(code: str):
    df = pd.read_parquet(HOURLY_DELAYS)
    rows = df[df["airport_code"] == code.upper()]
    return df_to_json_records(rows.sort_values("hour"))
