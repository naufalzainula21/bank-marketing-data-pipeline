from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType


def transform_education_status(df: DataFrame) -> DataFrame:
    return df.select("education_id", "value", "created_at", "updated_at")


def transform_marital_status(df: DataFrame) -> DataFrame:
    return df.select("marital_id", "value", "created_at", "updated_at")


def transform_marketing_campaign_deposit(df: DataFrame) -> DataFrame:
    balance_clean = F.regexp_replace(F.col("balance"), r"\$", "").cast(IntegerType())
    duration_in_year = F.floor(F.col("duration") / F.lit(365)).cast(IntegerType())

    return df.select(
        F.col("loan_data_id"),
        F.col("age"),
        F.col("job"),
        F.col("marital_id"),
        F.col("education_id"),
        F.col("default"),
        balance_clean.alias("balance"),
        F.col("housing"),
        F.col("loan"),
        F.col("contact"),
        F.col("day"),
        F.col("month"),
        F.col("duration"),
        duration_in_year.alias("duration_in_year"),
        F.col("campaign"),
        F.col("pdays").alias("days_since_last_campaign"),
        F.col("previous").alias("previous_campaign_contacts"),
        F.col("poutcome").alias("previous_campaign_outcome"),
        F.col("subscribed_deposit"),
        F.col("created_at"),
        F.col("updated_at"),
    )


def _parse_two_digit_year_date(col: F.Column, fmt: str = "d/M/yy") -> F.Column:
    """Parse a two-digit-year date and roll back any year > 2025 by 100.
    PySpark's d/M/yy under LEGACY parser maps yy=00..69 → 2000..2069 and
    yy=70..99 → 1970..1999, so a DOB of '21/6/65' parses as 2065-06-21.
    We subtract 100 years for any date past 2025 to land in the right century."""
    parsed = F.to_date(col, fmt)
    return F.when(F.year(parsed) > 2025, F.add_months(parsed, F.lit(-1200))).otherwise(parsed)


def transform_customers(df: DataFrame) -> DataFrame:
    gender = (
        F.when(F.col("CustGender") == "M", F.lit("Male"))
        .when(F.col("CustGender") == "F", F.lit("Female"))
        .otherwise(F.lit("Other"))
    )

    customers = df.select(
        F.col("CustomerID").alias("customer_id"),
        _parse_two_digit_year_date(F.col("CustomerDOB")).alias("birth_date"),
        gender.alias("gender"),
        F.col("CustLocation").alias("location"),
        F.col("CustAccountBalance").cast(DecimalType(18, 2)).alias("account_balance"),
    )

    return customers.dropDuplicates(["customer_id"])


def transform_transactions(df: DataFrame) -> DataFrame:
    transaction_time = F.date_format(
        F.to_timestamp(F.lpad(F.col("TransactionTime"), 6, "0"), "HHmmss"),
        "HH:mm:ss",
    )

    return df.select(
        F.col("TransactionID").alias("transaction_id"),
        F.col("CustomerID").alias("customer_id"),
        _parse_two_digit_year_date(F.col("TransactionDate")).alias("transaction_date"),
        transaction_time.alias("transaction_time"),
        F.col("TransactionAmount (INR)").cast(DecimalType(18, 2)).alias("transaction_amount"),
    )
