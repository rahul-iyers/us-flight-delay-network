"""
Phase 1a — Load & Clean
========================
Reads all CSVs from data/raw/, normalises column names to a canonical schema,
drops bad rows, parses timestamps, and writes flights_clean.parquet.

Usage:
    python pipeline/01_load_clean.py
"""

import sys
import duckdb
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    RAW_CSV_GLOB,
    CLEANED_PARQUET,
    PROCESSED_DIR,
    COLUMN_ALIASES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_column_map(columns: list[str]) -> dict[str, str]:
    """Return {raw_col -> canonical_col} for every recognised column."""
    col_set = set(c.upper() for c in columns)
    mapping: dict[str, str] = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.upper() in col_set:
                # find the real column name (case-sensitive)
                for c in columns:
                    if c.upper() == alias.upper():
                        mapping[c] = canonical
                        break
                break
    return mapping


def build_select_sql(col_map: dict[str, str]) -> str:
    """Build the SELECT clause that renames raw columns to canonical names."""
    parts = []
    for raw, canonical in col_map.items():
        if raw == canonical:
            parts.append(f'"{raw}"')
        else:
            parts.append(f'"{raw}" AS {canonical}')
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    print(f"[01] Scanning CSV files: {RAW_CSV_GLOB}")

    # Peek at the header to discover column names
    sample = con.execute(
        f"SELECT * FROM read_csv_auto('{RAW_CSV_GLOB}', sample_size=1)"
    ).description
    raw_columns = [col[0] for col in sample]
    print(f"[01] Found {len(raw_columns)} columns: {raw_columns[:10]}...")

    col_map = build_column_map(raw_columns)
    missing = [k for k in ["fl_date", "origin", "dest", "dep_delay", "arr_delay"] if k not in col_map.values()]
    if missing:
        print(f"[01] WARNING: essential columns not found: {missing}")
        print("[01]   Available columns:", raw_columns)

    select_clause = build_select_sql(col_map)

    # ------------------------------------------------------------------
    # Core cleaning query (runs entirely inside DuckDB — no pandas RAM)
    # ------------------------------------------------------------------
    # Determine which optional columns are present after aliasing
    aliased_canonicals = set(col_map.values())
    has_arr_delay   = "arr_delay"   in aliased_canonicals
    has_crs_dep     = "crs_dep_time" in aliased_canonicals
    has_dep_time    = "dep_time"    in aliased_canonicals

    # Build dep_hour expression from whichever time column is available
    if has_crs_dep:
        dep_hour_expr = """
        CASE
            WHEN crs_dep_time IS NOT NULL
             AND TRY_CAST(crs_dep_time AS INTEGER) IS NOT NULL
             AND CAST(crs_dep_time AS INTEGER) BETWEEN 0 AND 2359
            THEN CAST(CAST(crs_dep_time AS INTEGER) / 100 AS INTEGER)
            ELSE NULL
        END"""
    elif has_dep_time:
        dep_hour_expr = """
        CASE
            WHEN dep_time IS NOT NULL
             AND TRY_CAST(dep_time AS INTEGER) IS NOT NULL
             AND CAST(dep_time AS INTEGER) BETWEEN 0 AND 2359
            THEN CAST(CAST(dep_time AS INTEGER) / 100 AS INTEGER)
            ELSE NULL
        END"""
    else:
        dep_hour_expr = "NULL"

    arr_delay_flags = """
            CASE WHEN arr_delay >= 15 THEN 1 ELSE 0 END AS is_arr_delayed,""" if has_arr_delay else \
        "            0 AS is_arr_delayed,"

    clean_sql = f"""
    COPY (
        SELECT
            {select_clause},
            -- Derived: departure hour (0-23)
            ({dep_hour_expr}) AS dep_hour,
            -- Derived: is delayed (>=15 min)
            CASE WHEN CAST(dep_delay AS DOUBLE) >= 15 THEN 1 ELSE 0 END AS is_dep_delayed,
            {arr_delay_flags}
            -- Normalise cancelled flag to 0/1 integer
            CASE WHEN TRY_CAST(cancelled AS DOUBLE) > 0 THEN 1 ELSE 0 END AS is_cancelled
        FROM read_csv_auto('{RAW_CSV_GLOB}', ignore_errors=true)
        WHERE
            origin IS NOT NULL
            AND dest  IS NOT NULL
            AND origin != dest
            AND length(TRIM(origin)) = 3
            AND length(TRIM(dest))   = 3
            AND regexp_matches(UPPER(TRIM(origin)), '^[A-Z]{{3}}$')
            AND regexp_matches(UPPER(TRIM(dest)),   '^[A-Z]{{3}}$')
    )
    TO '{CLEANED_PARQUET}'
    (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 500000);
    """

    print("[01] Running cleaning query (this may take a few minutes for 26M rows)…")
    con.execute(clean_sql)

    # Quick sanity check
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{CLEANED_PARQUET}')").fetchone()
    print(f"[01] Done. {result[0]:,} clean rows written to {CLEANED_PARQUET}")

    con.close()


if __name__ == "__main__":
    main()
