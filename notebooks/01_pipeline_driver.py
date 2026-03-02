import sys
sys.path.append("../src")

from br_macro_lakehouse.config import LakehouseConfig
from br_macro_lakehouse.io.spark import ensure_catalog_schema, write_delta_table
from br_macro_lakehouse.logging import get_logger

from br_macro_lakehouse.sources.bcb_sgs_spark import fetch_usdbrl_daily_sgs_spark
from br_macro_lakehouse.sources.ibge_ipca_spark import fetch_ipca_index_monthly_spark
from br_macro_lakehouse.sources.worldbank_wdi_spark import fetch_tax_revenue_pct_gdp_spark

from br_macro_lakehouse.transforms.fx_spark import fx_daily_to_monthly
from br_macro_lakehouse.transforms.inflation_spark import add_inflation_rates
from br_macro_lakehouse.transforms.mart_spark import build_gold_mart_monthly

logger = get_logger()
cfg = LakehouseConfig()

SGS_CODE = 10813
FX_START = "01/01/2010"
FX_END   = "31/12/2025"

IPCA_START_YEAR = 2010
IPCA_END_YEAR   = 2025

WDI_COUNTRIES = ["BRA", "CHL", "COL", "MEX", "PER", "ZAF", "IDN", "IND", "VNM"]
TAX_START_YEAR = 2000
TAX_END_YEAR   = 2025

ensure_catalog_schema(spark, cfg.catalog, cfg.schema)

logger.info("Extracting FX (Spark JSON from BCB SGS)...")
fx_daily = fetch_usdbrl_daily_sgs_spark(spark, SGS_CODE, FX_START, FX_END)

logger.info("Extracting IPCA (Spark JSON from IBGE)...")
ipca = fetch_ipca_index_monthly_spark(spark, IPCA_START_YEAR, IPCA_END_YEAR)

logger.info("Extracting taxes (Spark JSON from WDI)...")
tax = fetch_tax_revenue_pct_gdp_spark(spark, WDI_COUNTRIES, TAX_START_YEAR, TAX_END_YEAR)

logger.info("Writing Bronze...")
write_delta_table(fx_daily, cfg.fqtn(cfg.bronze_fx_daily), mode="overwrite")
write_delta_table(ipca, cfg.fqtn(cfg.bronze_ipca_monthly), mode="overwrite")
write_delta_table(tax, cfg.fqtn(cfg.bronze_wdi_taxes), mode="overwrite")

logger.info("Building Silver...")
fx_monthly = fx_daily_to_monthly(fx_daily)
ipca_silver = add_inflation_rates(ipca)

write_delta_table(fx_monthly, cfg.fqtn(cfg.silver_fx_monthly), mode="overwrite")
write_delta_table(ipca_silver, cfg.fqtn(cfg.silver_ipca), mode="overwrite")

# Taxes are yearly already; store as Silver
write_delta_table(tax, cfg.fqtn(cfg.silver_taxes_yearly), mode="overwrite")

logger.info("Building Gold mart...")
gold = build_gold_mart_monthly(ipca_silver, fx_monthly, commodities=None)
write_delta_table(gold, cfg.fqtn(cfg.gold_mart_monthly), mode="overwrite")

display(spark.table(cfg.fqtn(cfg.gold_mart_monthly)).orderBy("month"))