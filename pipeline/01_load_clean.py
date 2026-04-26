import sys
import duckdb
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import *


def build_select_sql(col_map: dict[str, str]) -> str:
    parts = []
    for raw, canonical in col_map.items():
        if raw == canonical:
            parts.append(f'"{raw}"')
        else:
            parts.append(f'"{raw}" AS {canonical}')
    return ", ".join(parts)


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    print(f"[01] Reading {RAW_FILE_NAME}...")

    # Peek at the header to discover column names
    sample = con.execute(
        f"SELECT * FROM read_csv_auto('{RAW_DIR / RAW_FILE_NAME}', sample_size=1)"
    ).description
    raw_columns = [col[0] for col in sample]
    print(f"[01] Found {len(raw_columns)} columns: {raw_columns}")

    select_clause = build_select_sql(COLUMN_MAP)

    dep_hour_expr = """
    CASE
        WHEN crs_dep_time IS NOT NULL
            AND TRY_CAST(crs_dep_time AS INTEGER) IS NOT NULL
            AND CAST(crs_dep_time AS INTEGER) BETWEEN 0 AND 2359
        THEN CAST(CAST(crs_dep_time AS INTEGER) / 100 AS INTEGER)
        ELSE NULL
    END"""

    clean_sql = f"""
    COPY (
        SELECT
            {select_clause},
            -- Departure hour (0-23)
            ({dep_hour_expr}) AS dep_hour,
            -- Is delayed (>=15 min)
            CASE WHEN CAST(dep_delay AS DOUBLE) >= 15 THEN 1 ELSE 0 END AS is_dep_delayed,
            -- Normalise cancelled flag to 0/1 integer
            CASE WHEN TRY_CAST(cancelled AS DOUBLE) > 0 THEN 1 ELSE 0 END AS is_cancelled
        FROM read_csv_auto('{RAW_DIR / RAW_FILE_NAME}', ignore_errors=true)
        WHERE origin IS NOT NULL AND dest IS NOT NULL
    )
    TO '{CLEANED_PARQUET}'
    (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 500000);
    """

    print("[01] Running cleaning query...")
    con.execute(clean_sql)

    # Quick sanity check
    rowcount = con.execute(f"SELECT COUNT(*) FROM read_parquet('{CLEANED_PARQUET}')").fetchone()[0]
    print(f"[01] Done. {rowcount} rows written to {CLEANED_PARQUET.name}")

    con.close()


if __name__ == "__main__":
    main()
