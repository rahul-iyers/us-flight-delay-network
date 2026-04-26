from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

AIRPORT_STATS   = PROCESSED_DIR / "airport_stats.parquet"
ROUTE_STATS     = PROCESSED_DIR / "route_stats.parquet"
HOURLY_DELAYS   = PROCESSED_DIR / "hourly_delays.parquet"
AIRLINE_STATS   = PROCESSED_DIR / "airline_stats.parquet"
MONTHLY_STATS   = PROCESSED_DIR / "monthly_stats.parquet"
NETWORK_NODES   = PROCESSED_DIR / "network_nodes.parquet"
NETWORK_EDGES   = PROCESSED_DIR / "network_edges.parquet"
PROPAGATION     = PROCESSED_DIR / "propagation_edges.parquet"
PROP_SUMMARY    = PROCESSED_DIR / "propagation_summary.parquet"
GRAPH_JSON      = PROCESSED_DIR / "graph.json"

CORS_ORIGINS = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]
