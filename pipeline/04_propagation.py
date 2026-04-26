import sys
import duckdb
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import *


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
        dep_hour * 60                       AS dep_minute
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
    con = duckdb.connect()

    print("[04] Using temporal correlation propagation...")
    sql = TEMPORAL_PROPAGATION_SQL.format(
        src=str(CLEANED_PARQUET),
        window=PROPAGATION_WINDOW_MINUTES,
        threshold=PROPAGATION_DELAY_THRESHOLD,
    )

    copy_sql = f"""
    COPY ({sql})
    TO '{PROPAGATION_PARQUET}'
    (FORMAT PARQUET, COMPRESSION 'zstd')
    """
    con.execute(copy_sql)

    count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PROPAGATION_PARQUET}')"
    ).fetchone()[0]
    print(f"[04] Done. {count} rows written to {PROPAGATION_PARQUET.name}")

    # Write summary
    summary_sql = f"""
    COPY (
        {PROPAGATION_SUMMARY_SQL.format(prop=PROPAGATION_PARQUET)}
    )
    TO '{PROPAGATION_SUMMARY_PARQUET}'
    (FORMAT PARQUET, COMPRESSION 'zstd')
    """
    con.execute(summary_sql)

    summary_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PROPAGATION_SUMMARY_PARQUET}')"
    ).fetchone()[0]
    print(f"[04] Done. {summary_count} rows written to {PROPAGATION_SUMMARY_PARQUET.name}")

    con.close()
    print("[04] Propagation phase complete.")


if __name__ == "__main__":
    main()
