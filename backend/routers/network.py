# /network: graph JSON and airport/routes node/edge data.
import json
import math
from typing import Optional

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import Response

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import df_to_json_records

ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

NETWORK_NODES   = PROCESSED_DIR / "network_nodes.parquet"
NETWORK_EDGES   = PROCESSED_DIR / "network_edges.parquet"
GRAPH_JSON      = PROCESSED_DIR / "graph.json"


router = APIRouter()

def _sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


@router.get("/graph")
def get_graph(top_edges: int=2000, min_flights: int = 0):
    payload = json.loads(GRAPH_JSON.read_text())
    edges = payload["edges"]
    if min_flights:
        edges = [e for e in edges if (e.get("total_flights") or 0) >= min_flights]
    edges = sorted(edges, key=lambda e: e.get("total_flights") or 0, reverse=True)[:top_edges]
    result = _sanitize({"nodes": payload["nodes"], "edges": edges, "communities": payload.get("communities", {})})
    return Response(content=json.dumps(result), media_type="application/json")

# TODO: remove community if not being used
@router.get("/nodes")
def get_nodes(community: Optional[int]=None, limit: int=1000):
    df = pd.read_parquet(NETWORK_NODES)
    return df_to_json_records(df.head(limit))


@router.get("/nodes/{code}")
def get_node(code: str):
    df = pd.read_parquet(NETWORK_NODES)
    row = df[df["airport_code"] == code.upper()]
    return df_to_json_records(row)[0]


@router.get("/edges")
def get_edges(origin: Optional[str] = None, dest: Optional[str] = None, min_flights: int = 50, limit: int = 2000):
    df = pd.read_parquet(NETWORK_EDGES)
    if origin:
        df = df[df["origin"] == origin.upper()]
    if dest:
        df = df[df["dest"] == dest.upper()]
    if min_flights:
        df = df[df["total_flights"] >= min_flights]
    return df_to_json_records(df.nlargest(limit, "total_flights"))


@router.get("/communities")
def get_communities():
    df = pd.read_parquet(NETWORK_NODES)
    groups = {}
    for _, row in df.iterrows():
        cid = int(row.get("community_id", -1))
        groups.setdefault(cid, []).append(row["airport_code"])
    return groups
