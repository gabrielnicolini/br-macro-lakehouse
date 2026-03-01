# Databricks notebook source
import sys

sys.path.append("../src")

from br_macro_lakehouse.config import LakehouseConfig
from br_macro_lakehouse.logging import get_logger
from br_macro_lakehouse.io.spark import ensure_catalog_schema, write_delta_table

from br_macro_lakehouse.sources.bcb_sgs import fetch_usdbrl_daily_sgs
from br_macro_lakehouse.sources.ibge_ipca import fetch_ipca_index_monthly
from br_macro_lakehouse.sources.worldbank_wdi import fetch_tax_revenue_pct_gdp

from br_macro_lakehouse.transforms.fx import daily_to_monthly_fx
from br_macro_lakehouse.transforms.inflation import add_inflation_rates
from br_macro_lakehouse.transforms.taxes import clean_tax_series
from br_macro_lakehouse.transforms.mart import build_gold_mart_monthly

from br_macro_lakehouse.quality.checks import assert_month_monotonic

logger = get_logger()
cfg = LakehouseConfig()

SGS_CODE = 10813
FX_START = "01/01/2010"
FX_END = "31/12/2025"
IPCA_START_YEAR = 2010
IPCA_END_YEAR = 2025

WDI_COUNTRIES = ["BRA", "CHL", "COL", "MEX", "PER", "ZAF", "IDN", "IND", "VNM"]
TAX_START_YEAR = 2000
TAX_END_YEAR = 2025

ensure_catalog_schema(spark, cfg.catalog, cfg.schema)

logger.info("Extracting FX (BCB SGS)...")
fx_daily_pd = fetch_usdbrl_daily_sgs(SGS_CODE, FX_START, FX_END)

logger.info("Extracting IPCA (IBGE)...")
ipca_pd = fetch_ipca_index_monthly(IPCA_START_YEAR, IPCA_END_YEAR)

logger.info("Extracting taxes (World Bank WDI)...")
tax_pd = fetch_tax_revenue_pct_gdp(WDI_COUNTRIES, TAX_START_YEAR, TAX_END_YEAR)

logger.info("Writing Bronze tables...")
write_delta_table(
    spark.createDataFrame(fx_daily_pd), cfg.fqtn(cfg.bronze_fx_daily), mode="overwrite"
)
write_delta_table(
    spark.createDataFrame(ipca_pd), cfg.fqtn(cfg.bronze_ipca_monthly), mode="overwrite"
)
write_delta_table(
    spark.createDataFrame(tax_pd), cfg.fqtn(cfg.bronze_wdi_taxes), mode="overwrite"
)

logger.info("Building Silver datasets...")
fx_monthly_pd = daily_to_monthly_fx(fx_daily_pd)
ipca_silver_pd = add_inflation_rates(ipca_pd)
tax_silver_pd = clean_tax_series(tax_pd)

write_delta_table(
    spark.createDataFrame(fx_monthly_pd),
    cfg.fqtn(cfg.silver_fx_monthly),
    mode="overwrite",
)
write_delta_table(
    spark.createDataFrame(ipca_silver_pd), cfg.fqtn(cfg.silver_ipca), mode="overwrite"
)
write_delta_table(
    spark.createDataFrame(tax_silver_pd),
    cfg.fqtn(cfg.silver_taxes_yearly),
    mode="overwrite",
)

logger.info("Building Gold mart...")
gold_pd = build_gold_mart_monthly(ipca_silver_pd, fx_monthly_pd)
gold_pd = gold_pd.sort_values("month")
assert_month_monotonic(gold_pd)

write_delta_table(
    spark.createDataFrame(gold_pd), cfg.fqtn(cfg.gold_mart_monthly), mode="overwrite"
)

logger.info("Done.")
display(spark.table(cfg.fqtn(cfg.gold_mart_monthly)).orderBy("month"))
