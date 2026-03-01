from __future__ import annotations

import pandas as pd


def daily_to_monthly_fx(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["month"] = pd.to_datetime(out["date"]).dt.to_period("M").dt.to_timestamp()
    out = (
        out.groupby("month", as_index=False)["usdbrl"]
        .mean()
        .rename(columns={"usdbrl": "usdbrl_avg"})
    )
    out = out.sort_values("month")
    out["usdbrl_mom"] = out["usdbrl_avg"].pct_change(1)
    out["usdbrl_yoy"] = out["usdbrl_avg"].pct_change(12)
    return out
