from __future__ import annotations

import pandas as pd

from br_macro_lakehouse.io.http import get_json


def fetch_ipca_index_monthly(start_year: int, end_year: int) -> pd.DataFrame:
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/all/variaveis/2265?localidades=N1[all]"
    data = get_json(url)

    serie = data[0]["resultados"][0]["series"][0]["serie"]
    rows = [{"period": k, "ipca_index": float(v) if v is not None else None} for k, v in serie.items()]
    df = pd.DataFrame(rows)
    df["month"] = pd.to_datetime(df["period"], format="%Y%m", errors="coerce")
    df = df.dropna(subset=["month"])
    df = df[(df["month"].dt.year >= start_year) & (df["month"].dt.year <= end_year)]
    return df[["month", "ipca_index"]].sort_values("month")
