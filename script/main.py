from pyspark.sql import SparkSession

from helper.logging_helper import setup_logger
from extract.extract import (
    extract_education_status,
    extract_marital_status,
    extract_marketing_campaign_deposit,
    extract_bank_transaction_csv,
)
from transform.transform import (
    transform_education_status,
    transform_marital_status,
    transform_marketing_campaign_deposit,
    transform_customers,
    transform_transactions,
)
from load.load import (
    load_education_status,
    load_marital_status,
    load_marketing_campaign_deposit,
    load_customers,
    load_transactions,
)


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PacmannBankPipeline")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark


def run() -> None:
    logger = setup_logger("etl")
    logger.info("=== Pipeline start ===")

    spark = build_spark()
    logger.info("SparkSession ready")

    try:
        logger.info("Extract: education_status")
        raw_education = extract_education_status(spark)
        logger.info("Extract: marital_status")
        raw_marital = extract_marital_status(spark)
        logger.info("Extract: marketing_campaign_deposit")
        raw_campaign = extract_marketing_campaign_deposit(spark)
        logger.info("Extract: new_bank_transaction.csv")
        raw_csv = extract_bank_transaction_csv(spark)

        logger.info("Transform: education_status")
        education_df = transform_education_status(raw_education)
        logger.info("Transform: marital_status")
        marital_df = transform_marital_status(raw_marital)
        logger.info("Transform: marketing_campaign_deposit")
        campaign_df = transform_marketing_campaign_deposit(raw_campaign)
        logger.info("Transform: customers")
        customers_df = transform_customers(raw_csv)
        logger.info("Transform: transactions")
        transactions_df = transform_transactions(raw_csv)

        # Parent tables first so FKs (transactions -> customers,
        # marketing_campaign_deposit -> marital/education) resolve.
        logger.info("Load: education_status")
        load_education_status(education_df)
        logger.info("Load: marital_status")
        load_marital_status(marital_df)
        logger.info("Load: marketing_campaign_deposit")
        load_marketing_campaign_deposit(campaign_df)
        logger.info("Load: customers")
        load_customers(customers_df)
        logger.info("Load: transactions")
        load_transactions(transactions_df)

        logger.info("=== Pipeline success ===")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    run()
