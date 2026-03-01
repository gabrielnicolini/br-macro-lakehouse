from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    """Unity Catalog + table naming configuration."""

    catalog: str = "main"
    schema: str = "br_macro"

    # Bronze
    bronze_fx_daily: str = "bronze_fx_daily"
    bronze_ipca_monthly: str = "bronze_ipca_monthly"
    bronze_pinksheet_raw: str = "bronze_pinksheet_raw"
    bronze_wdi_taxes: str = "bronze_wdi_taxes"

    # Silver
    silver_fx_monthly: str = "silver_fx_monthly"
    silver_ipca: str = "silver_ipca"
    silver_commodities: str = "silver_commodities"
    silver_taxes_yearly: str = "silver_taxes_yearly"

    # Gold
    gold_mart_monthly: str = "gold_mart_monthly"

    def fqtn(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"
