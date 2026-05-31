from pyspark.sql import SparkSession, DataFrame

from helper.db_connection import source_jdbc


def _read_source_table(spark: SparkSession, table: str) -> DataFrame:
    url, props = source_jdbc()
    return (
        spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )


def extract_education_status(spark: SparkSession) -> DataFrame:
    return _read_source_table(spark, "public.education_status")


def extract_marital_status(spark: SparkSession) -> DataFrame:
    return _read_source_table(spark, "public.marital_status")


def extract_marketing_campaign_deposit(spark: SparkSession) -> DataFrame:
    return _read_source_table(spark, "public.marketing_campaign_deposit")


def extract_bank_transaction_csv(spark: SparkSession, path: str = "data/new_bank_transaction.csv") -> DataFrame:
    return spark.read.csv(path, header=True)
