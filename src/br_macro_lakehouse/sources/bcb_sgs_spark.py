from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def fetch_usdbrl_daily_sgs_spark(
    spark: SparkSession, series_code: int, start_ddmmyyyy: str, end_ddmmyyyy: str
) -> DataFrame:
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
        f"?formato=json&dataInicial={start_ddmmyyyy}&dataFinal={end_ddmmyyyy}"
    )
    df = spark.read.json(url)
    return (
        df.select(
            F.to_date(F.col("data"), "dd/MM/yyyy").alias("date"),
            F.col("valor").cast("double").alias("usdbrl"),
        )
        .where("date is not null")
        .orderBy("date")
    )