from __future__ import annotations

import pandas as pd


def assert_month_monotonic(df: pd.DataFrame) -> None:
    if "month" not in df.columns:
        raise ValueError("Missing required column: month")
    if not pd.Series(df["month"]).is_monotonic_increasing:
        raise ValueError("Column 'month' is not monotonic increasing")
