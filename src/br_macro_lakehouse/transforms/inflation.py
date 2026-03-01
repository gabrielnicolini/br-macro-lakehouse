from __future__ import annotations

import pandas as pd


def add_inflation_rates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values("month")
    out["ipca_mom"] = out["ipca_index"].pct_change(1)
    out["ipca_yoy"] = out["ipca_index"].pct_change(12)
    return out
