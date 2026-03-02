from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def fetch_tax_revenue_pct_gdp_spark(
    spark: SparkSession, country_ids: list[str], start_year: int, end_year: int
) -> DataFrame:
    indicator = "GC.TAX.TOTL.GD.ZS"
    countries = ";".join(country_ids)
    url = f"https://api.worldbank.org/v2/country/{countries}/indicator/{indicator}?format=json&per_page=20000"

    raw = spark.read.json(url)

    # raw[1] is the array of records
    rows = raw.select(F.explode(F.col("_corrupt_record")).alias("x"))  # fallback if needed

    # More robust: read as text then json? In most DBR setups, below works:
    data = spark.read.json(url).select(F.col("value")).limit(0)  # placeholder

    # Safer approach: use read.json then select element 1
    arr = spark.read.json(url)
    df = arr.select(F.col("`1`").alias("rows")).select(F.explode("rows").alias("r"))

    out = (
        df.select(
            F.col("r.country.id").alias("country_id"),
            F.col("r.country.value").alias("country"),
            F.col("r.date").cast("int").alias("year"),
            F.col("r.value").cast("double").alias("tax_to_gdp"),
        )
        .where((F.col("year") >= start_year) & (F.col("year") <= end_year))
        .orderBy("country_id", "year")
    )
    return out