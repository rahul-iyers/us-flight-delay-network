"""
Phase 1b — Aggregate
=====================
Reads flights_clean.parquet and produces smaller summary parquets:
  - airport_stats.parquet
  - route_stats.parquet
  - hourly_delays.parquet
  - airline_stats.parquet
  - monthly_stats.parquet

All queries run entirely in DuckDB.

Usage:
    python pipeline/02_aggregate.py
"""

import sys
import json
import duckdb
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEANED_PARQUET,
    AIRPORT_STATS_PARQUET,
    ROUTE_STATS_PARQUET,
    HOURLY_DELAYS_PARQUET,
    AIRLINE_STATS_PARQUET,
    MONTHLY_STATS_PARQUET,
    PROCESSED_DIR,
)
from airports_metadata import AIRPORTS


def write_parquet(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')")
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    print(f"    → {count:,} rows  →  {path.name}")
    return count


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    src = str(CLEANED_PARQUET)
    print(f"[02] Source: {src}")

    # ------------------------------------------------------------------
    # 1. Airport stats
    # ------------------------------------------------------------------
    print("[02] Building airport_stats…")
    # Detect optional columns
    src_cols = set(r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}') LIMIT 0").fetchall())
    has_arr   = "arr_delay"   in src_cols
    has_city  = "origin_city" in src_cols
    has_state = "origin_state" in src_cols
    has_cd    = "carrier_delay" in src_cols
    has_wd    = "weather_delay" in src_cols
    has_nd    = "nas_delay"     in src_cols
    has_la    = "late_aircraft" in src_cols

    arr_avg    = "AVG(arr_delay)"   if has_arr  else "NULL::DOUBLE"
    city_col   = "ANY_VALUE(origin_city)"  if has_city  else "NULL::VARCHAR"
    state_col  = "ANY_VALUE(origin_state)" if has_state else "NULL::VARCHAR"
    cd_avg     = "AVG(carrier_delay)" if has_cd else "NULL::DOUBLE"
    wd_avg     = "AVG(weather_delay)" if has_wd else "NULL::DOUBLE"
    nd_avg     = "AVG(nas_delay)"     if has_nd else "NULL::DOUBLE"
    la_avg     = "AVG(late_aircraft)" if has_la else "NULL::DOUBLE"

    airport_sql = f"""
    SELECT
        origin                                      AS airport_code,
        COUNT(*)                                    AS total_flights,
        AVG(CAST(dep_delay AS DOUBLE))              AS avg_dep_delay,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(dep_delay AS DOUBLE))
                                                    AS median_dep_delay,
        {arr_avg}                                   AS avg_arr_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(is_dep_delayed) / COUNT(*)::DOUBLE      AS dep_delay_rate,
        SUM(CASE WHEN CAST(dep_delay AS DOUBLE) <= 0 THEN 1 ELSE 0 END) / COUNT(*)::DOUBLE
                                                    AS on_time_rate,
        {cd_avg}                                    AS avg_carrier_delay,
        {wd_avg}                                    AS avg_weather_delay,
        {nd_avg}                                    AS avg_nas_delay,
        {la_avg}                                    AS avg_late_aircraft_delay,
        COUNT(DISTINCT airline)                     AS num_airlines,
        COUNT(DISTINCT dest)                        AS num_destinations,
        {city_col}                                  AS city,
        {state_col}                                 AS state
    FROM read_parquet('{src}')
    WHERE is_cancelled = 0
    GROUP BY origin
    HAVING COUNT(*) >= 10
    ORDER BY total_flights DESC
    """
    write_parquet(con, airport_sql, AIRPORT_STATS_PARQUET)

    # Enrich with coordinates from metadata
    df = pd.read_parquet(AIRPORT_STATS_PARQUET)
    meta_rows = []
    for code in df["airport_code"]:
        info = AIRPORTS.get(code.upper())
        if info:
            lat, lon, name, city, state = info
            meta_rows.append({"airport_code": code, "lat": lat, "lon": lon, "full_name": name})
        else:
            meta_rows.append({"airport_code": code, "lat": None, "lon": None, "full_name": None})
    meta_df = pd.DataFrame(meta_rows)
    df = df.merge(meta_df, on="airport_code", how="left")
    # Fill city/state from metadata if missing
    df["city"] = df.apply(
        lambda r: AIRPORTS.get(r["airport_code"], (None, None, None, r["city"], r["state"]))[3]
        if pd.isna(r["city"]) else r["city"], axis=1
    )
    df["state"] = df.apply(
        lambda r: AIRPORTS.get(r["airport_code"], (None, None, None, None, r["state"]))[4]
        if pd.isna(r["state"]) else r["state"], axis=1
    )
    df.to_parquet(AIRPORT_STATS_PARQUET, compression="zstd", index=False)
    print(f"    coordinates added for {df['lat'].notna().sum()} / {len(df)} airports")

    # ------------------------------------------------------------------
    # 2. Route stats
    # ------------------------------------------------------------------
    print("[02] Building route_stats…")
    has_dist = "distance" in src_cols
    dist_col = "AVG(CAST(distance AS DOUBLE))" if has_dist else "NULL::DOUBLE"

    route_sql = f"""
    SELECT
        origin,
        dest,
        COUNT(*)                                    AS total_flights,
        AVG(CAST(dep_delay AS DOUBLE))              AS avg_dep_delay,
        {arr_avg}                                   AS avg_arr_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(is_dep_delayed) / COUNT(*)::DOUBLE      AS dep_delay_rate,
        {dist_col}                                  AS avg_distance,
        COUNT(DISTINCT airline)                     AS num_airlines
    FROM read_parquet('{src}')
    WHERE is_cancelled = 0
    GROUP BY origin, dest
    HAVING COUNT(*) >= 5
    ORDER BY total_flights DESC
    """
    write_parquet(con, route_sql, ROUTE_STATS_PARQUET)

    # ------------------------------------------------------------------
    # 3. Hourly delay distributions (per airport × hour)
    # ------------------------------------------------------------------
    print("[02] Building hourly_delays…")
    hourly_sql = f"""
    SELECT
        origin                                                          AS airport_code,
        dep_hour                                                        AS hour,
        COUNT(*)                                                        AS flight_count,
        AVG(CAST(dep_delay AS DOUBLE))                                  AS avg_dep_delay,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(dep_delay AS DOUBLE))
                                                                        AS p25_dep_delay,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(dep_delay AS DOUBLE))
                                                                        AS p75_dep_delay,
        {arr_avg}                                                       AS avg_arr_delay,
        SUM(is_dep_delayed) / COUNT(*)::DOUBLE                          AS dep_delay_rate
    FROM read_parquet('{src}')
    WHERE is_cancelled = 0
      AND dep_hour IS NOT NULL
    GROUP BY origin, dep_hour
    ORDER BY origin, dep_hour
    """
    write_parquet(con, hourly_sql, HOURLY_DELAYS_PARQUET)

    # ------------------------------------------------------------------
    # 4. Airline stats
    # ------------------------------------------------------------------
    print("[02] Building airline_stats…")
    has_aname = "airline_name" in src_cols
    aname_col = "ANY_VALUE(airline_name)" if has_aname else "NULL::VARCHAR"

    airline_sql = f"""
    SELECT
        airline                                     AS airline_code,
        {aname_col}                                 AS airline_full_name,
        COUNT(*)                                    AS total_flights,
        AVG(CAST(dep_delay AS DOUBLE))              AS avg_dep_delay,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(dep_delay AS DOUBLE))
                                                    AS median_dep_delay,
        {arr_avg}                                   AS avg_arr_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(is_dep_delayed) / COUNT(*)::DOUBLE      AS dep_delay_rate,
        SUM(CASE WHEN CAST(dep_delay AS DOUBLE) <= 0 THEN 1 ELSE 0 END) / COUNT(*)::DOUBLE
                                                    AS on_time_rate,
        COUNT(DISTINCT origin)                      AS airports_served,
        COUNT(DISTINCT origin || '-' || dest)       AS routes_served
    FROM read_parquet('{src}')
    WHERE is_cancelled = 0
    GROUP BY airline
    ORDER BY total_flights DESC
    """
    write_parquet(con, airline_sql, AIRLINE_STATS_PARQUET)

    # ------------------------------------------------------------------
    # 5. Monthly stats (for timeline / trend view)
    # ------------------------------------------------------------------
    print("[02] Building monthly_stats…")

    # Check which optional columns are available in the cleaned parquet
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}') LIMIT 0").fetchall()]
    date_col = next((c for c in cols if c in ("fl_date", "Date", "date", "flight_date")), None)
    if date_col:
        monthly_sql = f"""
        SELECT
            strftime(TRY_CAST("{date_col}" AS DATE), '%Y-%m')
                                                        AS month,
            COUNT(*)                                    AS total_flights,
            AVG(CAST(dep_delay AS DOUBLE))              AS avg_dep_delay,
            {arr_avg}                                   AS avg_arr_delay,
            SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
            SUM(is_dep_delayed) / COUNT(*)::DOUBLE      AS dep_delay_rate
        FROM read_parquet('{src}')
        WHERE is_cancelled = 0
          AND TRY_CAST("{date_col}" AS DATE) IS NOT NULL
        GROUP BY month
        ORDER BY month
        """
    else:
        monthly_sql = "SELECT 'no_date' AS month, 0 AS total_flights, 0.0 AS avg_dep_delay, 0.0 AS avg_arr_delay, 0.0 AS cancellation_rate, 0.0 AS dep_delay_rate"

    write_parquet(con, monthly_sql, MONTHLY_STATS_PARQUET)

    con.close()
    print("[02] Aggregation complete.")


if __name__ == "__main__":
    main()
