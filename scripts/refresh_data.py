from __future__ import annotations

import json
import os
from datetime import date, datetime

import pandas as pd
import requests


ROOT = os.path.dirname(os.path.dirname(__file__))
OUT_DIR = os.path.join(ROOT, "data", "silver")
os.makedirs(OUT_DIR, exist_ok=True)


def _get_json(url: str, params: dict) -> list | dict:
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _bcb_sgs_daily_series(series_id: int, start: str, end: str) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
    payload = _get_json(url, {"formato": "json", "dataInicial": start, "dataFinal": end})
    df = pd.DataFrame(payload)
    # Expect columns: data (dd/mm/yyyy), valor (string)
    df["date"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["value"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[["date", "value"]].dropna().sort_values("date")
    return df


def _bcb_sgs_download_in_10y_windows(series_id: int, start_year: int, end_year: int) -> pd.DataFrame:
    # BCB daily series: max 10-year window per request.
    chunks: list[pd.DataFrame] = []
    for y in range(start_year, end_year + 1, 10):
        y0 = y
        y1 = min(y + 9, end_year)
        start = f"01/01/{y0}"
        end = f"31/12/{y1}"
        chunks.append(_bcb_sgs_daily_series(series_id, start, end))
    df = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["date"]).sort_values("date")
    return df


def _to_monthly_avg(df_daily: pd.DataFrame, col_name: str) -> pd.DataFrame:
    out = df_daily.copy()
    out["month"] = out["date"].dt.to_period("M").dt.to_timestamp()
    out = out.groupby("month", as_index=False)["value"].mean()
    out = out.rename(columns={"value": col_name})
    out["mom"] = out[col_name].pct_change(1)
    out["yoy"] = out[col_name].pct_change(12)
    return out


def _ibge_ipca_12m() -> pd.DataFrame:
    # IBGE API (aggregate 1737, variable 2265) -> IPCA 12-month accumulated (%)
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/all/variaveis/2265"
    params = {"localidades": "N1[all]"}
    payload = _get_json(url, params)

    # Shape: list with one dict, payload[0]["resultados"][0]["series"][0]["serie"] is dict-like
    serie = payload[0]["resultados"][0]["series"][0]["serie"]

    rows = []
    for k, v in serie.items():
        # k like "202401"
        if not (k and k.isdigit() and len(k) == 6):
            continue
        try:
            val = float(v)
        except Exception:
            continue
        month = pd.Timestamp(datetime.strptime(k, "%Y%m")).replace(day=1)
        rows.append((month, val))

    df = pd.DataFrame(rows, columns=["month", "ipca_12m"])
    df = df.sort_values("month")
    df["ipca_mom"] = df["ipca_12m"].pct_change(1)
    df["ipca_yoy"] = df["ipca_12m"].pct_change(12)
    df["month"] = pd.to_datetime(df["month"])
    return df


def main() -> None:
    today = date.today()
    start_year = 2000
    end_year = today.year

    # 1) FX: USD/BRL (SGS 10813)
    fx_daily = _bcb_sgs_download_in_10y_windows(series_id=10813, start_year=start_year, end_year=end_year)
    fx_monthly = _to_monthly_avg(fx_daily, col_name="usdbrl_avg")

    # 2) IPCA 12m
    ipca = _ibge_ipca_12m()

    # --- Force identical merge key (month grain) ---
    ipca["month_key"] = pd.to_datetime(ipca["month"]).dt.strftime("%Y-%m")
    fx_monthly["month_key"] = pd.to_datetime(fx_monthly["month"]).dt.strftime("%Y-%m")

    mart = pd.merge(ipca.drop(columns=["month"]), fx_monthly.drop(columns=["month"]), on="month_key", how="left")
    mart["month"] = pd.to_datetime(mart["month_key"] + "-01")
    mart = mart[["month"] + [c for c in mart.columns if c != "month"]]

    # Output
    fx_monthly.to_csv(os.path.join(OUT_DIR, "fx_usdbrl_monthly.csv"), index=False)
    ipca.to_csv(os.path.join(OUT_DIR, "ipca_12m_monthly.csv"), index=False)
    mart.to_csv(os.path.join(OUT_DIR, "mart_monthly.csv"), index=False)


if __name__ == "__main__":
    main()