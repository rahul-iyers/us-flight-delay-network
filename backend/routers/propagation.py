"""
/propagation  — delay propagation chains.
"""
from typing import Optional
from functools import lru_cache

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PROP_SUMMARY
from utils import df_to_json_records

router = APIRouter()


@lru_cache(maxsize=1)
def _load_summary() -> pd.DataFrame:
    if not PROP_SUMMARY.exists():
        raise FileNotFoundError("propagation_summary.parquet not found. Run the pipeline first.")
    return pd.read_parquet(PROP_SUMMARY)


@router.get("/summary")
def get_propagation_summary(
    hub: Optional[str] = Query(None),
    dest: Optional[str] = Query(None),
    min_count: int = Query(5, ge=1),
    limit: int = Query(500, ge=1, le=5000),
):
    df = _load_summary()
    if hub:
        df = df[df["hub_airport"] == hub.upper()]
    if dest:
        df = df[df["outbound_dest"] == dest.upper()]
    df = df[df["propagation_count"] >= min_count]
    df = df.nlargest(limit, "propagation_count")
    return df_to_json_records(df)


@router.get("/airport/{code}")
def get_airport_propagation(code: str):
    df = _load_summary()
    code = code.upper()
    as_hub = df_to_json_records(df[df["hub_airport"] == code].nlargest(20, "propagation_count"))
    as_dest = df_to_json_records(df[df["outbound_dest"] == code].nlargest(20, "propagation_count"))
    if not as_hub and not as_dest:
        raise HTTPException(status_code=404, detail=f"No propagation data for '{code}'")
    return {"airport": code, "as_hub": as_hub, "as_destination": as_dest}


@router.get("/top-hubs")
def get_top_hubs(limit: int = Query(20, ge=1, le=100)):
    df = _load_summary()
    agg = (
        df.groupby("hub_airport")
        .agg(
            total_propagations=("propagation_count", "sum"),
            unique_destinations=("outbound_dest", "nunique"),
            avg_outbound_delay=("avg_outbound_delay", "mean"),
        )
        .reset_index()
        .nlargest(limit, "total_propagations")
    )
    return df_to_json_records(agg)
