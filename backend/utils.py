"""Shared serialization helpers."""
import math
import pandas as pd


def df_to_json_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to a list of JSON-safe dicts (nan → None)."""
    records = df.to_dict(orient="records")
    return [_clean(r) for r in records]


def _clean(record: dict) -> dict:
    out = {}
    for k, v in record.items():
        if isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif isinstance(v, float) and math.isinf(v):
            out[k] = None
        else:
            out[k] = v
    return out
