from __future__ import annotations

import pandas as pd

from br_macro_lakehouse.io.http import get_json


def fetch_usdbrl_daily_sgs(series_code: int, start_ddmmyyyy: str, end_ddmmyyyy: str) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    params = {"formato": "json", "dataInicial": start_ddmmyyyy, "dataFinal": end_ddmmyyyy}
    data = get_json(url, params=params)

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df["usdbrl"] = pd.to_numeric(df["valor"], errors="coerce")
    return df[["date", "usdbrl"]].dropna(subset=["date"]).sort_values("date")
