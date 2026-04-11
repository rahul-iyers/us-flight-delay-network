"""
Phase 2 — Network Graph Construction
=====================================
Builds a directed airport network from route_stats.parquet:
  - Nodes: airports with centrality metrics
  - Edges: routes weighted by avg delay / flight volume
  - Community detection via Louvain algorithm
  - Outputs: network_nodes.parquet, network_edges.parquet, graph.json

Usage:
    python pipeline/03_network.py
"""

import sys
import json
import math
import numpy as np
import pandas as pd
import networkx as nx
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    AIRPORT_STATS_PARQUET,
    ROUTE_STATS_PARQUET,
    NETWORK_NODES_PARQUET,
    NETWORK_EDGES_PARQUET,
    GRAPH_JSON,
    PROCESSED_DIR,
)

try:
    import community as community_louvain  # python-louvain
    HAS_LOUVAIN = True
except ImportError:
    HAS_LOUVAIN = False
    print("[03] WARNING: python-louvain not installed; skipping community detection.")
    print("     Install with: pip install python-louvain")


def build_graph(routes: pd.DataFrame) -> nx.DiGraph:
    """Build a directed weighted graph from route_stats DataFrame."""
    G = nx.DiGraph()
    for _, row in routes.iterrows():
        G.add_edge(
            row["origin"],
            row["dest"],
            weight=float(row["total_flights"]),
            avg_dep_delay=float(row["avg_dep_delay"]) if not math.isnan(row["avg_dep_delay"]) else 0.0,
            avg_arr_delay=float(row["avg_arr_delay"]) if not math.isnan(row["avg_arr_delay"]) else 0.0,
            cancellation_rate=float(row["cancellation_rate"]) if not math.isnan(row["cancellation_rate"]) else 0.0,
            avg_distance=float(row["avg_distance"]) if not math.isnan(row["avg_distance"]) else 0.0,
        )
    return G


def compute_centrality(G: nx.DiGraph) -> dict[str, dict]:
    """Compute several centrality metrics; return dict keyed by node."""
    print("[03]   degree centrality…")
    deg = nx.degree_centrality(G)

    print("[03]   in-degree / out-degree…")
    in_deg = dict(G.in_degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight"))

    # Betweenness on the undirected projection (too slow on full directed graph)
    print("[03]   betweenness centrality (undirected projection)…")
    G_und = G.to_undirected()
    bet = nx.betweenness_centrality(G_und, normalized=True, weight="weight")

    print("[03]   PageRank…")
    pr = nx.pagerank(G, weight="weight", alpha=0.85)

    # Combine
    nodes = set(G.nodes())
    result: dict[str, dict] = {}
    for n in nodes:
        result[n] = {
            "degree_centrality": deg.get(n, 0.0),
            "in_degree_weighted": in_deg.get(n, 0.0),
            "out_degree_weighted": out_deg.get(n, 0.0),
            "betweenness_centrality": bet.get(n, 0.0),
            "pagerank": pr.get(n, 0.0),
        }
    return result


def detect_communities(G: nx.DiGraph) -> dict[str, int]:
    """Run Louvain community detection on the undirected projection."""
    if not HAS_LOUVAIN:
        return {n: 0 for n in G.nodes()}

    G_und = G.to_undirected()
    # Add edge weight = log(flights+1) so dominant hubs don't overwhelm
    for u, v, d in G_und.edges(data=True):
        d["weight"] = math.log1p(d.get("weight", 1.0))

    partition = community_louvain.best_partition(G_und, weight="weight", random_state=42)
    return partition


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("[03] Loading route stats…")
    routes = pd.read_parquet(ROUTE_STATS_PARQUET)
    airports = pd.read_parquet(AIRPORT_STATS_PARQUET)
    print(f"[03]   {len(routes):,} routes, {len(airports):,} airports")

    print("[03] Building directed graph…")
    G = build_graph(routes)
    print(f"[03]   {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print("[03] Computing centrality metrics…")
    centrality = compute_centrality(G)

    print("[03] Detecting communities (Louvain)…")
    communities = detect_communities(G)
    num_communities = len(set(communities.values()))
    print(f"[03]   {num_communities} communities found")

    # ------------------------------------------------------------------
    # Build network_nodes DataFrame
    # ------------------------------------------------------------------
    ap_index = airports.set_index("airport_code")
    node_rows = []
    for node in G.nodes():
        c = centrality.get(node, {})
        row = {
            "airport_code": node,
            "community_id": communities.get(node, -1),
            "degree_centrality": c.get("degree_centrality", 0.0),
            "in_degree_weighted": c.get("in_degree_weighted", 0.0),
            "out_degree_weighted": c.get("out_degree_weighted", 0.0),
            "betweenness_centrality": c.get("betweenness_centrality", 0.0),
            "pagerank": c.get("pagerank", 0.0),
        }
        # Merge airport stats
        if node in ap_index.index:
            ap = ap_index.loc[node]
            row.update({
                "total_flights": int(ap.get("total_flights", 0)),
                "avg_dep_delay": float(ap.get("avg_dep_delay", 0)),
                "avg_arr_delay": float(ap.get("avg_arr_delay", 0)),
                "cancellation_rate": float(ap.get("cancellation_rate", 0)),
                "on_time_rate": float(ap.get("on_time_rate", 0)),
                "lat": float(ap["lat"]) if pd.notna(ap.get("lat")) else None,
                "lon": float(ap["lon"]) if pd.notna(ap.get("lon")) else None,
                "city": str(ap.get("city", "")),
                "state": str(ap.get("state", "")),
                "full_name": str(ap.get("full_name", "")),
                "num_airlines": int(ap.get("num_airlines", 0)),
                "num_destinations": int(ap.get("num_destinations", 0)),
            })
        node_rows.append(row)

    nodes_df = pd.DataFrame(node_rows)
    nodes_df.to_parquet(NETWORK_NODES_PARQUET, compression="zstd", index=False)
    print(f"[03]   network_nodes.parquet: {len(nodes_df):,} rows")

    # ------------------------------------------------------------------
    # Build network_edges DataFrame (just annotate routes with centrality)
    # ------------------------------------------------------------------
    edges_df = routes.copy()
    # Delay "severity" score for edge colouring: 0-1 scale
    max_delay = edges_df["avg_dep_delay"].quantile(0.95)
    edges_df["delay_severity"] = (edges_df["avg_dep_delay"].clip(upper=max_delay) / max(max_delay, 1)).round(4)
    edges_df.to_parquet(NETWORK_EDGES_PARQUET, compression="zstd", index=False)
    print(f"[03]   network_edges.parquet: {len(edges_df):,} rows")

    # ------------------------------------------------------------------
    # Produce graph.json for the frontend (trimmed for payload size)
    # ------------------------------------------------------------------
    print("[03] Writing graph.json…")

    # Only keep airports with coordinates for the map
    map_nodes = nodes_df[nodes_df["lat"].notna() & nodes_df["lon"].notna()]

    # Top N edges by flight volume (keep graph manageable in browser)
    TOP_EDGES = 3000
    top_edges = edges_df.nlargest(TOP_EDGES, "total_flights")

    def clean_records(df: pd.DataFrame, cols: list[str]) -> list[dict]:
        """Convert DataFrame to records with NaN/Inf → None for JSON safety."""
        import math
        present = [c for c in cols if c in df.columns]
        rows = df[present].to_dict(orient="records")
        result = []
        for row in rows:
            cleaned = {}
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v
            result.append(cleaned)
        return result

    # Note: use post-rename column names ("id" not "airport_code", "source"/"target" not "origin"/"dest")
    node_cols = [
        "id", "lat", "lon", "city", "state", "full_name",
        "total_flights", "avg_dep_delay", "avg_arr_delay",
        "cancellation_rate", "on_time_rate",
        "degree_centrality", "betweenness_centrality", "pagerank",
        "community_id", "num_airlines", "num_destinations",
    ]
    edge_cols = [
        "source", "target", "total_flights", "avg_dep_delay",
        "cancellation_rate", "delay_severity", "avg_distance",
    ]

    node_records = clean_records(map_nodes.rename(columns={"airport_code": "id"}), node_cols)
    edge_records = clean_records(
        top_edges.rename(columns={"origin": "source", "dest": "target"}),
        edge_cols,
    )

    graph_payload = {
        "nodes": node_records,
        "edges": edge_records,
        "communities": {
            str(cid): int(pd.Series(communities).eq(cid).sum())
            for cid in sorted(set(communities.values()))
        },
    }

    GRAPH_JSON.write_text(json.dumps(graph_payload, indent=None))
    size_kb = GRAPH_JSON.stat().st_size / 1024
    print(f"[03]   graph.json: {len(graph_payload['nodes'])} nodes, {len(graph_payload['edges'])} edges  ({size_kb:.0f} KB)")
    print("[03] Network phase complete.")


if __name__ == "__main__":
    main()
