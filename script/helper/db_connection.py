import os
from dotenv import load_dotenv
import psycopg2

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))


def _jdbc_props(user: str, password: str) -> dict:
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def source_jdbc() -> tuple[str, dict]:
    """JDBC URL + connection properties for the source Postgres."""
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    return url, _jdbc_props(user, password)


def warehouse_jdbc() -> tuple[str, dict]:
    """JDBC URL + connection properties for the data warehouse Postgres."""
    host = os.getenv("DB_HOST_TARGET")
    port = os.getenv("DB_PORT_TARGET")
    db = os.getenv("DB_NAME_TARGET")
    user = os.getenv("DB_USER_TARGET")
    password = os.getenv("DB_PASS_TARGET")
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    return url, _jdbc_props(user, password)


def warehouse_psycopg2_conn():
    """psycopg2 connection to the warehouse — used to TRUNCATE before append."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST_TARGET"),
        port=os.getenv("DB_PORT_TARGET"),
        dbname=os.getenv("DB_NAME_TARGET"),
        user=os.getenv("DB_USER_TARGET"),
        password=os.getenv("DB_PASS_TARGET"),
    )
