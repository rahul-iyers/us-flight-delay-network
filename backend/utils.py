import math
import pandas as pd


def df_to_json_records(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    return [clean(r) for r in records]


def clean(record):
    out = {}
    for k, v in record.items():
        if isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif isinstance(v, float) and math.isinf(v):
            out[k] = None
        else:
            out[k] = v
    return out
