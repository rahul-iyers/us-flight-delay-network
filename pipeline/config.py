"""Shared configuration for all pipeline steps."""
from pathlib import Path

# Absolute project root (one level up from this file)
ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"

# The main CSV (BTS On-Time Performance data)
# Users should place their CSV(s) here, e.g.:
#   data/raw/flights.csv   (single file)  OR
#   data/raw/flights_*.csv (multiple shards)
RAW_CSV_GLOB = str(RAW_DIR / "*.csv")

# Output parquet paths
CLEANED_PARQUET = PROCESSED_DIR / "flights_clean.parquet"
AIRPORT_STATS_PARQUET = PROCESSED_DIR / "airport_stats.parquet"
ROUTE_STATS_PARQUET = PROCESSED_DIR / "route_stats.parquet"
HOURLY_DELAYS_PARQUET = PROCESSED_DIR / "hourly_delays.parquet"
AIRLINE_STATS_PARQUET = PROCESSED_DIR / "airline_stats.parquet"
MONTHLY_STATS_PARQUET = PROCESSED_DIR / "monthly_stats.parquet"
NETWORK_NODES_PARQUET = PROCESSED_DIR / "network_nodes.parquet"
NETWORK_EDGES_PARQUET = PROCESSED_DIR / "network_edges.parquet"
PROPAGATION_PARQUET = PROCESSED_DIR / "propagation_edges.parquet"
GRAPH_JSON = PROCESSED_DIR / "graph.json"

# Delay propagation parameters
PROPAGATION_WINDOW_MINUTES = 90   # max turnaround window to infer propagation
PROPAGATION_DELAY_THRESHOLD = 15  # minutes — both flights must exceed this

# Column name normalization: we accept both "old style" (ALL_CAPS) and
# "new style" (CamelCase / PascalCase) BTS column names.
COLUMN_ALIASES: dict[str, list[str]] = {
    # Date
    "fl_date":        ["FL_DATE", "FlightDate", "Date", "date", "flight_date"],
    # Carrier/airline
    "airline":        ["OP_CARRIER", "Reporting_Airline", "UNIQUE_CARRIER", "Carrier", "carrier", "airline_code"],
    "airline_name":   ["Airline Name", "airline_name", "AirlineName", "AIRLINE_NAME", "carrier_name"],
    # Tail number (optional — not in all datasets)
    "tail_num":       ["TAIL_NUM", "Tail_Number", "tail_num", "TailNumber"],
    # Flight number
    "flight_num":     ["FLIGHT_NUMBER_REPORTING_AIRLINE", "FlightNumber", "Flight_Num", "flight_num"],
    # Origin / destination
    "origin":         ["ORIGIN", "Origin", "origin"],
    "dest":           ["DEST", "Dest", "dest"],
    # Scheduled departure time (HHMM integer)
    "crs_dep_time":   ["CRS_DEP_TIME", "CRSDepTime", "Dep_Time", "dep_time_sched", "ScheduledDep"],
    # Actual departure time (HHMM integer)
    "dep_time":       ["DEP_TIME", "DepTime", "Actual_Dep", "ActualDep", "actual_dep"],
    # Departure delay in minutes (positive = late)
    "dep_delay":      ["DEP_DELAY", "DepDelay", "DEP_DELAY_NEW", "DepDelayMinutes", "Delay", "delay", "dep_delay_min"],
    # Arrival times/delay — may be absent
    "arr_time":       ["ARR_TIME", "ArrTime", "Actual_Arr", "ActualArr"],
    "arr_delay":      ["ARR_DELAY", "ArrDelay", "ARR_DELAY_NEW", "ArrDelayMinutes", "arr_delay_min"],
    # Cancelled flag (0/1 or True/False)
    "cancelled":      ["CANCELLED", "Cancelled", "cancelled", "cancel"],
    # Distance in miles — optional
    "distance":       ["DISTANCE", "Distance", "distance_miles"],
    # City/state — optional, used for display labels
    "origin_city":    ["ORIGIN_CITY_NAME", "OriginCityName", "origin_city"],
    "origin_state":   ["ORIGIN_STATE_ABR", "OriginState", "OriginStateName", "origin_state"],
    "dest_city":      ["DEST_CITY_NAME", "DestCityName", "dest_city"],
    "dest_state":     ["DEST_STATE_ABR", "DestState", "DestStateName", "dest_state"],
    # Delay-cause breakdowns — optional
    "carrier_delay":  ["CARRIER_DELAY", "CarrierDelay"],
    "weather_delay":  ["WEATHER_DELAY", "WeatherDelay"],
    "nas_delay":      ["NAS_DELAY", "NASDelay"],
    "late_aircraft":  ["LATE_AIRCRAFT_DELAY", "LateAircraftDelay"],
}
