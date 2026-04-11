"""
Phase 3 — Delay Propagation Algorithm
=======================================
For each flight, determine whether the delay was "propagated" from an inbound
flight on the same aircraft (tail number matching) or via temporal proximity
at a shared airport.

Algorithm:
  Primary  — Tail-number chaining:
    Group flights by tail_num + date. For consecutive flights where the
    aircraft turns around within PROPAGATION_WINDOW_MINUTES, if the inbound
    flight had dep_delay >= threshold AND the outbound also had dep_delay >=
    threshold, record a propagation edge (origin_of_inbound → dest_of_inbound
    = origin_of_outbound → dest_of_outbound).

  Fallback — Temporal correlation (used when tail_num is unavailable):
    For each airport, find pairs of arrival/departure within the window where
    both flights are significantly delayed.

Output: propagation_edges.parquet with columns:
    propagation_id, inbound_origin, hub_airport, outbound_dest,
    inbound_arr_delay, outbound_dep_delay, turnaround_minutes,
    fl_date, airline, tail_num, method

Usage:
    python pipeline/04_propagation.py
"""

import sys
import duckdb
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEANED_PARQUET,
    PROPAGATION_PARQUET,
    PROCESSED_DIR,
    PROPAGATION_WINDOW_MINUTES,
    PROPAGATION_DELAY_THRESHOLD,
)


TAIL_NUM_PROPAGATION_SQL = """
-- Tail-number chaining: same aircraft, consecutive legs, both delayed
WITH ranked AS (
    SELECT
        tail_num,
        fl_date,
        origin,
        dest,
        airline,
        -- Reconstruct a sortable minute-of-day from the scheduled time string
        -- crs_dep_time and arr_time are stored as HHMM integers
        CAST(crs_dep_time AS INTEGER) / 100 * 60 + CAST(crs_dep_time AS INTEGER) % 100
            AS dep_minute,
        COALESCE(
            CAST(arr_time AS INTEGER) / 100 * 60 + CAST(arr_time AS INTEGER) % 100,
            CAST(crs_dep_time AS INTEGER) / 100 * 60 + CAST(crs_dep_time AS INTEGER) % 100
        )                       AS arr_minute,
        dep_delay,
        arr_delay,
        ROW_NUMBER() OVER (PARTITION BY tail_num, fl_date ORDER BY crs_dep_time)
            AS leg
    FROM read_parquet('{src}')
    WHERE tail_num IS NOT NULL
      AND tail_num != ''
      AND is_cancelled = 0
      AND crs_dep_time IS NOT NULL
),
consecutive AS (
    SELECT
        a.tail_num,
        a.fl_date,
        a.origin          AS inbound_origin,
        a.dest            AS hub_airport,
        b.dest            AS outbound_dest,
        a.arr_delay       AS inbound_arr_delay,
        b.dep_delay       AS outbound_dep_delay,
        b.dep_minute - a.arr_minute  AS turnaround_minutes,
        a.airline,
        'tail_num'        AS method
    FROM ranked  a
    JOIN ranked  b
      ON  a.tail_num = b.tail_num
      AND a.fl_date  = b.fl_date
      AND b.leg      = a.leg + 1
      AND (b.dep_minute - a.arr_minute) BETWEEN 0 AND {window}
    WHERE a.arr_delay  >= {threshold}
      AND b.dep_delay  >= {threshold}
)
SELECT
    ROW_NUMBER() OVER ()            AS propagation_id,
    inbound_origin,
    hub_airport,
    outbound_dest,
    inbound_arr_delay,
    outbound_dep_delay,
    turnaround_minutes,
    fl_date,
    airline,
    tail_num,
    method
FROM consecutive
"""


TEMPORAL_PROPAGATION_SQL = """
-- Temporal / departure-only fallback:
-- Same airport + same airline + same date, two consecutive delayed departures.
-- The earlier flight's delay "propagates" to the later one departing 30-90 min later.
WITH dep_col AS (
    SELECT
        origin,
        dest,
        fl_date,
        airline,
        CAST(dep_delay AS DOUBLE)           AS dep_delay,
        is_cancelled,
        dep_hour * 60                       AS dep_minute   -- hour-granularity from aggregated data
    FROM read_parquet('{src}')
    WHERE is_cancelled = 0
      AND dep_hour IS NOT NULL
),
delayed AS (
    SELECT * FROM dep_col
    WHERE dep_delay >= {threshold}
      AND dep_minute IS NOT NULL
),
matched AS (
    SELECT
        a.origin        AS inbound_origin,
        a.dest          AS hub_airport,
        b.dest          AS outbound_dest,
        a.dep_delay     AS inbound_arr_delay,   -- proxy: inbound dep delay
        b.dep_delay     AS outbound_dep_delay,
        b.dep_minute - a.dep_minute AS turnaround_minutes,
        a.fl_date,
        a.airline,
        NULL::VARCHAR   AS tail_num,
        'temporal'      AS method
    FROM delayed a
    JOIN delayed b
      ON  a.dest         = b.origin         -- a arrives at hub, b departs from same hub
      AND a.fl_date      = b.fl_date
      AND a.airline      = b.airline
      AND (b.dep_minute - a.dep_minute) BETWEEN 30 AND {window}
)
SELECT
    ROW_NUMBER() OVER ()  AS propagation_id,
    inbound_origin,
    hub_airport,
    outbound_dest,
    inbound_arr_delay,
    outbound_dep_delay,
    turnaround_minutes,
    fl_date,
    airline,
    tail_num,
    method
FROM matched
"""


PROPAGATION_SUMMARY_SQL = """
-- Summarise propagation edges into a per-route count table
SELECT
    hub_airport,
    outbound_dest,
    method,
    COUNT(*)                    AS propagation_count,
    AVG(inbound_arr_delay)      AS avg_inbound_delay,
    AVG(outbound_dep_delay)     AS avg_outbound_delay,
    AVG(turnaround_minutes)     AS avg_turnaround_minutes,
    COUNT(DISTINCT airline)     AS airlines_affected
FROM read_parquet('{prop}')
GROUP BY hub_airport, outbound_dest, method
ORDER BY propagation_count DESC
"""


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    src = str(CLEANED_PARQUET)

    con = duckdb.connect()

    # Check which columns are present
    cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{src}') LIMIT 0"
    ).fetchall()}
    has_tail    = "tail_num" in cols
    has_arr     = "arr_time" in cols or "arr_delay" in cols
    has_dep_sched = "crs_dep_time" in cols

    if has_tail and has_dep_sched and has_arr:
        print("[04] Running tail-number propagation algorithm…")
        sql = TAIL_NUM_PROPAGATION_SQL.format(
            src=src,
            window=PROPAGATION_WINDOW_MINUTES,
            threshold=PROPAGATION_DELAY_THRESHOLD,
        )
        method = "tail_num"
    else:
        print("[04] Using temporal correlation propagation…")
        print(f"      (tail_num={has_tail}, arr_time/delay={has_arr}, crs_dep={has_dep_sched})")
        sql = TEMPORAL_PROPAGATION_SQL.format(
            src=src,
            window=PROPAGATION_WINDOW_MINUTES,
            threshold=PROPAGATION_DELAY_THRESHOLD,
        )
        method = "temporal"

    copy_sql = f"""
    COPY (
        {sql}
    )
    TO '{PROPAGATION_PARQUET}'
    (FORMAT PARQUET, COMPRESSION 'zstd')
    """

    con.execute(copy_sql)

    count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PROPAGATION_PARQUET}')"
    ).fetchone()[0]
    print(f"[04]   {count:,} propagation events written ({method})")

    # Write summary
    summary_path = PROCESSED_DIR / "propagation_summary.parquet"
    con.execute(f"""
    COPY (
        {PROPAGATION_SUMMARY_SQL.format(prop=PROPAGATION_PARQUET)}
    )
    TO '{summary_path}'
    (FORMAT PARQUET, COMPRESSION 'zstd')
    """)
    summary_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{summary_path}')"
    ).fetchone()[0]
    print(f"[04]   {summary_count:,} propagation summary rows → propagation_summary.parquet")

    con.close()
    print("[04] Propagation phase complete.")


if __name__ == "__main__":
    main()
