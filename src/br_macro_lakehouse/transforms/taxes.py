from __future__ import annotations

import pandas as pd


def clean_tax_series(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tax_to_gdp"] = pd.to_numeric(out["tax_to_gdp"], errors="coerce")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    return out.dropna(subset=["country_id", "year"]).sort_values(["country_id", "year"])
