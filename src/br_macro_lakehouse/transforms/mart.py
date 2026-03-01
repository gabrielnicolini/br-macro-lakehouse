from __future__ import annotations

import pandas as pd


def build_gold_mart_monthly(
    ipca: pd.DataFrame, fx_monthly: pd.DataFrame
) -> pd.DataFrame:
    df = ipca.merge(fx_monthly, on="month", how="left").sort_values("month")
    for lag in [1, 3, 6, 12]:
        df[f"usdbrl_mom_lag{lag}"] = df["usdbrl_mom"].shift(lag)
    return df
