from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def fetch_ipca_index_monthly_spark(spark: SparkSession, start_year: int, end_year: int) -> DataFrame:
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/all/variaveis/2265?localidades=N1[all]"
    raw = spark.read.json(url)

    # Explode nested structure safely
    # raw[0].resultados[0].series[0].serie is a map: { "YYYYMM": "value" }
    serie_map = raw.select(
        F.col("resultados")[0]["series"][0]["serie"].alias("serie")
    )

    df = (
        serie_map
        .select(F.explode("serie").alias("period", "value"))
        .select(
            F.to_date(F.col("period"), "yyyyMM").alias("month"),
            F.col("value").cast("double").alias("ipca_index"),
        )
        .where("month is not null")
        .where((F.year("month") >= start_year) & (F.year("month") <= end_year))
        .orderBy("month")
    )
    return df