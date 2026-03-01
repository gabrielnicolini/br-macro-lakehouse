from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def ensure_catalog_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def write_delta_table(df: DataFrame, fqtn: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).format("delta").saveAsTable(fqtn)
