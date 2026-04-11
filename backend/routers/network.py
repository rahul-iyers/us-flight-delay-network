"""
/network  — pre-built graph JSON and node/edge data.
"""
import json
import math
from functools import lru_cache
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import NETWORK_NODES, NETWORK_EDGES, GRAPH_JSON
from utils import df_to_json_records

router = APIRouter()


@lru_cache(maxsize=1)
def _load_nodes() -> pd.DataFrame:
    if not NETWORK_NODES.exists():
        raise FileNotFoundError("network_nodes.parquet not found. Run the pipeline first.")
    return pd.read_parquet(NETWORK_NODES)


@lru_cache(maxsize=1)
def _load_edges() -> pd.DataFrame:
    if not NETWORK_EDGES.exists():
        raise FileNotFoundError("network_edges.parquet not found.")
    return pd.read_parquet(NETWORK_EDGES)


@lru_cache(maxsize=1)
def _load_graph_json() -> dict:
    if not GRAPH_JSON.exists():
        raise FileNotFoundError("graph.json not found. Run the pipeline first.")
    return json.loads(GRAPH_JSON.read_text())


def _sanitize(obj):
    """Recursively replace NaN/Inf with None for JSON safety."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


@router.get("/graph")
def get_graph(
    top_edges: int = Query(2000, ge=100, le=5000),
    min_flights: int = Query(0, ge=0),
):
    payload = _load_graph_json()
    edges = payload["edges"]
    if min_flights:
        edges = [e for e in edges if (e.get("total_flights") or 0) >= min_flights]
    edges = sorted(edges, key=lambda e: e.get("total_flights") or 0, reverse=True)[:top_edges]
    result = _sanitize({"nodes": payload["nodes"], "edges": edges, "communities": payload.get("communities", {})})
    # Use Response to bypass FastAPI's own serializer (which rejects nan)
    return Response(content=json.dumps(result), media_type="application/json")


@router.get("/nodes")
def get_nodes(
    community: Optional[int] = Query(None),
    limit: int = Query(1000, ge=1, le=5000),
):
    df = _load_nodes()
    if community is not None:
        df = df[df["community_id"] == community]
    return df_to_json_records(df.head(limit))


@router.get("/nodes/{code}")
def get_node(code: str):
    df = _load_nodes()
    row = df[df["airport_code"] == code.upper()]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Node '{code}' not found")
    return df_to_json_records(row)[0]


@router.get("/edges")
def get_edges(
    origin: Optional[str] = Query(None),
    dest: Optional[str] = Query(None),
    min_flights: int = Query(50, ge=0),
    limit: int = Query(2000, ge=1, le=10000),
):
    df = _load_edges()
    if origin:
        df = df[df["origin"] == origin.upper()]
    if dest:
        df = df[df["dest"] == dest.upper()]
    if min_flights:
        df = df[df["total_flights"] >= min_flights]
    return df_to_json_records(df.nlargest(limit, "total_flights"))


@router.get("/communities")
def get_communities():
    df = _load_nodes()
    if "community_id" not in df.columns:
        return {}
    groups: dict[int, list] = {}
    for _, row in df.iterrows():
        cid = int(row.get("community_id", -1))
        groups.setdefault(cid, []).append(row["airport_code"])
    return groups
