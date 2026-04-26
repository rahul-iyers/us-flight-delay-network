from pathlib import Path

# Directories
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
RAW_FILE_NAME = "flight_data.csv"

# Output parquet paths
CLEANED_PARQUET = PROCESSED_DIR / "flights_clean.parquet"
AIRPORT_STATS_PARQUET = PROCESSED_DIR / "airport_stats.parquet"
ROUTE_STATS_PARQUET = PROCESSED_DIR / "route_stats.parquet"
HOURLY_DELAYS_PARQUET = PROCESSED_DIR / "hourly_delays.parquet"
AIRLINE_STATS_PARQUET = PROCESSED_DIR / "airline_stats.parquet"
NETWORK_NODES_PARQUET = PROCESSED_DIR / "network_nodes.parquet"
NETWORK_EDGES_PARQUET = PROCESSED_DIR / "network_edges.parquet"
PROPAGATION_PARQUET = PROCESSED_DIR / "propagation_edges.parquet"
PROPAGATION_SUMMARY_PARQUET = PROCESSED_DIR / "propagation_summary.parquet"
GRAPH_JSON = PROCESSED_DIR / "graph.json"

# Delay propagation parameters
PROPAGATION_WINDOW_MINUTES = 90   # max turnaround window to infer propagation
PROPAGATION_DELAY_THRESHOLD = 15  # minutes — both flights must exceed this

# Standardize csv column names
COLUMN_MAP = {
    "Date": "fl_date",
    "Carrier": "airline",
    "Airline Name": "airline_name",
    "Flight_Num": "flight_num",
    "Origin": "origin",
    "Dest": "dest",
    "Dep_Time": "crs_dep_time",
    "Actual_Dep": "dep_time",
    "Delay": "dep_delay",
    "Cancelled": "cancelled",
}
