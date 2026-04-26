import sys
import duckdb
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import *
from airports_metadata import AIRPORTS


def write_parquet(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')")
    rowcount = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    print(f"[02] Done. {rowcount} rows written to {path.name}")
    return rowcount


def main():
    con = duckdb.connect()

    print(f"[02] Reading {CLEANED_PARQUET.name}...")

    print("[02] Building airport_stats...")
    airport_sql = f"""
    SELECT
        origin                                      AS airport_code,
        COUNT(*)                                    AS total_flights,
        AVG(CASE WHEN is_cancelled = 0 THEN CAST(dep_delay AS DOUBLE) END)
                                                    AS avg_dep_delay,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_cancelled = 0 THEN CAST(dep_delay AS DOUBLE) END)
                                                    AS median_dep_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(CASE WHEN is_cancelled = 0 THEN is_dep_delayed ELSE 0 END) / NULLIF(SUM(CASE WHEN is_cancelled = 0 THEN 1 ELSE 0 END), 0)::DOUBLE
                                                    AS dep_delay_rate,
        SUM(CASE WHEN is_cancelled = 0 AND CAST(dep_delay AS DOUBLE) <= 0 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN is_cancelled = 0 THEN 1 ELSE 0 END), 0)::DOUBLE
                                                    AS on_time_rate,
        COUNT(DISTINCT airline)                     AS num_airlines,
        COUNT(DISTINCT dest)                        AS num_destinations,
    FROM read_parquet('{CLEANED_PARQUET}')
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
            meta_rows.append({
                "airport_code": code,
                "lat": lat,
                "lon": lon,
                "full_name": name,
                "city": city,
                "state": state
            })
        else:
            meta_rows.append({
                "airport_code": code,
                "lat": None,
                "lon": None,
                "full_name": None,
                "city": None,
                "state": None
            })
    meta_df = pd.DataFrame(meta_rows)
    df = df.merge(meta_df, on="airport_code", how="left")
    df.to_parquet(AIRPORT_STATS_PARQUET, compression="zstd", index=False)
    print(f"[02] Added metadata for {df['lat'].notna().sum()} / {len(df)} airports")


    print("[02] Building route_stats...")

    route_sql = f"""
    SELECT
        origin,
        dest,
        COUNT(*)                                    AS total_flights,
        AVG(CASE WHEN is_cancelled = 0 THEN CAST(dep_delay AS DOUBLE) END)
                                                    AS avg_dep_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(CASE WHEN is_cancelled = 0 THEN is_dep_delayed ELSE 0 END) / NULLIF(SUM(CASE WHEN is_cancelled = 0 THEN 1 ELSE 0 END), 0)::DOUBLE
                                                    AS dep_delay_rate,
        COUNT(DISTINCT airline)                     AS num_airlines
    FROM read_parquet('{CLEANED_PARQUET}')
    GROUP BY origin, dest
    HAVING COUNT(*) >= 5
    ORDER BY total_flights DESC
    """
    write_parquet(con, route_sql, ROUTE_STATS_PARQUET)


    print("[02] Building hourly_delays...")
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
        SUM(is_dep_delayed) / COUNT(*)::DOUBLE                          AS dep_delay_rate
    FROM read_parquet('{CLEANED_PARQUET}')
    WHERE is_cancelled = 0
      AND dep_hour IS NOT NULL
    GROUP BY origin, dep_hour
    ORDER BY origin, dep_hour
    """
    write_parquet(con, hourly_sql, HOURLY_DELAYS_PARQUET)


    print("[02] Building airline_stats...")
    airline_sql = f"""
    SELECT
        airline                                     AS airline_code,
        ANY_VALUE(airline_name)                     AS airline_full_name,
        COUNT(*)                                    AS total_flights,
        AVG(CASE WHEN is_cancelled = 0 THEN CAST(dep_delay AS DOUBLE) END)
                                                    AS avg_dep_delay,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_cancelled = 0 THEN CAST(dep_delay AS DOUBLE) END)
                                                    AS median_dep_delay,
        SUM(is_cancelled) / COUNT(*)::DOUBLE        AS cancellation_rate,
        SUM(CASE WHEN is_cancelled = 0 THEN is_dep_delayed ELSE 0 END) / NULLIF(SUM(CASE WHEN is_cancelled = 0 THEN 1 ELSE 0 END), 0)::DOUBLE
                                                    AS dep_delay_rate,
        SUM(CASE WHEN is_cancelled = 0 AND CAST(dep_delay AS DOUBLE) <= 0 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN is_cancelled = 0 THEN 1 ELSE 0 END), 0)::DOUBLE
                                                    AS on_time_rate,
        COUNT(DISTINCT origin)                      AS airports_served,
        COUNT(DISTINCT origin || '-' || dest)       AS routes_served
    FROM read_parquet('{CLEANED_PARQUET}')
    GROUP BY airline
    ORDER BY total_flights DESC
    """
    write_parquet(con, airline_sql, AIRLINE_STATS_PARQUET)

    con.close()
    print("[02] Aggregation complete.")


if __name__ == "__main__":
    main()
