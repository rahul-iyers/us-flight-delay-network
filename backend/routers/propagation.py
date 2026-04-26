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
from config import PROP_SUMMARY, NETWORK_NODES
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


@lru_cache(maxsize=1)
def _load_airport_meta() -> dict:
    try:
        cols = ["airport_code", "city", "state", "lat", "lon"]
        df = pd.read_parquet(NETWORK_NODES, columns=cols)
        return df.set_index("airport_code").to_dict("index")
    except Exception:
        return {}


@router.get("/tree")
def get_propagation_tree(
    airport: str = Query(..., description="Root airport IATA code"),
    hops: int = Query(3, ge=1, le=4),
    min_count: int = Query(50, ge=1),
    max_per_node: int = Query(8, ge=1, le=15),
    max_nodes: int = Query(50, ge=5, le=200),
):
    """Return flat node list for a multi-hop propagation tree rooted at an airport."""
    df = _load_summary()
    meta = _load_airport_meta()
    airport = airport.upper()

    def node_meta(code: str) -> dict:
        m = meta.get(code, {})
        lat = m.get("lat")
        lon = m.get("lon")
        return {
            "city": str(m.get("city", "") or ""),
            "state": str(m.get("state", "") or ""),
            "lat": float(lat) if lat is not None and not (isinstance(lat, float) and pd.isna(lat)) else None,
            "lon": float(lon) if lon is not None and not (isinstance(lon, float) and pd.isna(lon)) else None,
        }

    # Root node
    root_rows = df[df["hub_airport"] == airport]
    root_total = int(root_rows["propagation_count"].sum()) if not root_rows.empty else 0
    root_avg = float(root_rows["avg_outbound_delay"].mean()) if not root_rows.empty else 0.0

    flat_nodes = [{
        "id": airport,
        "parent": None,
        "airport": airport,
        "hop": 0,
        "avg_delay": round(root_avg, 1),
        "propagation_count": root_total,
        "avg_inbound_delay": 0.0,
        "avg_turnaround": 0.0,
        "airlines_affected": 0,
        **node_meta(airport),
    }]

    visited = {airport}
    current = [airport]

    per_hop_cap = [max_per_node, max(3, max_per_node // 2), max(2, max_per_node // 4), 1]

    for hop_num in range(1, hops + 1):
        if len(flat_nodes) >= max_nodes:
            break
        cap = per_hop_cap[hop_num - 1]
        next_level = []
        for src in current:
            if len(flat_nodes) >= max_nodes:
                break
            rows = (
                df[
                    (df["hub_airport"] == src) &
                    (df["propagation_count"] >= min_count) &
                    (~df["outbound_dest"].isin(visited))
                ]
                .nlargest(cap, "propagation_count")
            )
            for _, r in rows.iterrows():
                if len(flat_nodes) >= max_nodes:
                    break
                dest = str(r["outbound_dest"])
                visited.add(dest)
                next_level.append(dest)
                flat_nodes.append({
                    "id": dest,
                    "parent": src,
                    "airport": dest,
                    "hop": hop_num,
                    "avg_delay": round(float(r["avg_outbound_delay"]), 1),
                    "propagation_count": int(r["propagation_count"]),
                    "avg_inbound_delay": round(float(r["avg_inbound_delay"]), 1),
                    "avg_turnaround": round(float(r["avg_turnaround_minutes"]), 1),
                    "airlines_affected": int(r["airlines_affected"]),
                    **node_meta(dest),
                })
        current = next_level
        if not current:
            break

    return {"root": airport, "node_count": len(flat_nodes), "nodes": flat_nodes}
