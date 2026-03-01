from __future__ import annotations

import pandas as pd

from br_macro_lakehouse.io.http import get_json


def fetch_tax_revenue_pct_gdp(
    country_ids: list[str], start_year: int, end_year: int
) -> pd.DataFrame:
    indicator = "GC.TAX.TOTL.GD.ZS"
    country = ";".join(country_ids)
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000}

    data = get_json(url, params=params)
    rows = data[1] if isinstance(data, list) and len(data) > 1 else []

    out = []
    for r in rows:
        try:
            year = int(r["date"])
        except Exception:
            continue
        if start_year <= year <= end_year:
            out.append(
                {
                    "country_id": r["country"]["id"],
                    "country": r["country"]["value"],
                    "year": year,
                    "tax_to_gdp": r["value"],
                }
            )
    return pd.DataFrame(out).sort_values(["country_id", "year"])
