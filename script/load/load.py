from pyspark.sql import DataFrame

from helper.db_connection import warehouse_jdbc, warehouse_psycopg2_conn


def _truncate(table: str) -> None:
    """TRUNCATE the warehouse table (with CASCADE to handle FKs) before append."""
    conn = warehouse_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE public."{table}" RESTART IDENTITY CASCADE;')
        conn.commit()
    finally:
        conn.close()


def _append(df: DataFrame, table: str) -> None:
    url, props = warehouse_jdbc()
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", f"public.{table}")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .mode("append")
        .save()
    )


def load_table(df: DataFrame, table: str) -> None:
    """Truncate target table, then append the DataFrame."""
    _truncate(table)
    _append(df, table)


def load_education_status(df: DataFrame) -> None:
    load_table(df, "education_status")


def load_marital_status(df: DataFrame) -> None:
    load_table(df, "marital_status")


def load_marketing_campaign_deposit(df: DataFrame) -> None:
    load_table(df, "marketing_campaign_deposit")


def load_customers(df: DataFrame) -> None:
    load_table(df, "customers")


def load_transactions(df: DataFrame) -> None:
    load_table(df, "transactions")
