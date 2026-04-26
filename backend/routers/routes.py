# /routes - origin to destination route stats.
from typing import Optional

import pandas as pd
from fastapi import APIRouter

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import df_to_json_records

ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

ROUTE_STATS = PROCESSED_DIR / "route_stats.parquet"

router = APIRouter()


@router.get("/")
def list_routes(
    origin: Optional[str] = None,
    dest: Optional[str] = None,
    min_flights: int = 50,
    limit: int = 1000,
):
    df = pd.read_parquet(ROUTE_STATS)
    if origin:
        df = df[df["origin"] == origin.upper()]
    if dest:
        df = df[df["dest"] == dest.upper()]
    df = df[df["total_flights"] >= min_flights]
    df = df.nlargest(limit, "total_flights")
    return df_to_json_records(df)


@router.get("/{origin}/{dest}")
def get_route(origin: str, dest: str):
    df = pd.read_parquet(ROUTE_STATS)
    row = df[(df["origin"] == origin.upper()) & (df["dest"] == dest.upper())]
    return df_to_json_records(row)[0]
