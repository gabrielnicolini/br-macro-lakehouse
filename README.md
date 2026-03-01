# br-macro-lakehouse

A small, production-style Databricks Lakehouse project that relates Brazilian inflation (IPCA) to:
- FX (USD/BRL)
- global agricultural commodity prices
- tax revenue comparisons across emerging economies

The project follows a Delta Lake Bronze/Silver/Gold architecture and is designed to be easy to extend.

## Data sources (open data)
- **Inflation (IPCA)**: IBGE (SIDRA API)
- **USD/BRL FX**: Central Bank of Brazil (BCB SGS API)
- **Global commodities**: World Bank "Pink Sheet" (monthly XLS)
- **Taxes**: World Bank WDI (Tax revenue % of GDP)

## Architecture
- **Bronze**: raw ingested datasets (as received)
- **Silver**: cleaned/standardized datasets (typed, normalized dates, monthly frequency)
- **Gold**: analytics-ready monthly mart (features + lags)

## Quickstart (Databricks)
1. Create a Databricks Repo from this Git repository.
2. Run `notebooks/01_pipeline_driver.py` to build Bronze/Silver/Gold tables.
3. Run `notebooks/02_analysis_report.py` for example plots.

## Default Unity Catalog location
Tables are written under `main.br_macro` by default (configurable in `src/br_macro_lakehouse/config.py`).

## License
MIT
