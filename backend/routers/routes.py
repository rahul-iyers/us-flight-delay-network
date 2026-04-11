"""
/routes  — origin→destination route stats.
"""
from typing import Optional
from functools import lru_cache

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import ROUTE_STATS
from utils import df_to_json_records

router = APIRouter()


@lru_cache(maxsize=1)
def _load_routes() -> pd.DataFrame:
    if not ROUTE_STATS.exists():
        raise FileNotFoundError("route_stats.parquet not found. Run the pipeline first.")
    return pd.read_parquet(ROUTE_STATS)


@router.get("/")
def list_routes(
    origin: Optional[str] = Query(None),
    dest: Optional[str] = Query(None),
    min_flights: int = Query(50, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
):
    df = _load_routes()
    if origin:
        df = df[df["origin"] == origin.upper()]
    if dest:
        df = df[df["dest"] == dest.upper()]
    if min_flights:
        df = df[df["total_flights"] >= min_flights]
    df = df.nlargest(limit, "total_flights")
    return df_to_json_records(df)


@router.get("/{origin}/{dest}")
def get_route(origin: str, dest: str):
    df = _load_routes()
    row = df[(df["origin"] == origin.upper()) & (df["dest"] == dest.upper())]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Route {origin}→{dest} not found")
    return df_to_json_records(row)[0]
