# Bank Marketing Data Pipeline (PySpark)

A PySpark ETL pipeline that loads bank marketing & transaction data from a
source Postgres database and a CSV file into a Postgres data warehouse.
Built for the Pacmann Week 6 — Building Data Pipeline with Python and PySpark.

---

## How this implementation answers the task

### 1. Tech stack
| Requirement | Where it lives |
|---|---|
| PySpark `SparkSession` | [script/main.py:28](script/main.py) — `build_spark()` |
| `spark.sql.legacy.timeParserPolicy = LEGACY` | [script/main.py:33](script/main.py) |
| CSV read via `spark.read.csv(..., header=True)` | [script/extract/extract.py:33](script/extract/extract.py) — `extract_bank_transaction_csv()` |
| Postgres via JDBC driver | [script/helper/db_connection.py](script/helper/db_connection.py) + [driver/postgresql-42.6.0.jar](driver/postgresql-42.6.0.jar) |
| Load strategy: truncate → append | [script/load/load.py:6](script/load/load.py) — `_truncate()` then `_append()` |
| Logging to `log/` with timestamp | [script/helper/logging_helper.py](script/helper/logging_helper.py) → `script/log/etl_<YYYYMMDD_HHMMSS>.log` |

### 2. File structure (matches the task spec)
```
script/
├── helper/
│   ├── db_connection.py      # reusable JDBC connection helper
│   └── logging_helper.py     # reusable logging setup
├── extract/
│   └── extract.py            # all extract functions
├── transform/
│   └── transform.py          # all transform functions
├── load/
│   └── load.py               # all load functions
├── main.py                   # orchestrates extract → transform → load
├── data/new_bank_transaction.csv/   # input CSV (Spark part files)
└── log/                      # ETL run logs (gitignored)
```

### 3. Source → Target mapping coverage

#### `education_status` (source DB → DW) — copy as-is
- [transform_education_status()](script/transform/transform.py) selects all 4 columns unchanged.

#### `marital_status` (source DB → DW) — copy as-is
- [transform_marital_status()](script/transform/transform.py) selects all 4 columns unchanged.

#### `marketing_campaign_deposit` (source DB → DW)
| Source | Target | Transformation | Implementation |
|---|---|---|---|
| `balance` | `balance` | strip `$`, cast INT | `F.regexp_replace(..., "\$", "").cast(IntegerType())` |
| `duration` | `duration_in_year` | `floor(duration/365)`, INT | `F.floor(col("duration")/365).cast(IntegerType())` |
| `pdays` | `days_since_last_campaign` | rename | `.alias("days_since_last_campaign")` |
| `previous` | `previous_campaign_contacts` | rename | `.alias("previous_campaign_contacts")` |
| `poutcome` | `previous_campaign_outcome` | rename | `.alias("previous_campaign_outcome")` |
| all others | — | copy as-is | direct `F.col(...)` |

See [transform_marketing_campaign_deposit()](script/transform/transform.py).

#### `customers` (CSV → DW)
| Source | Target | Transformation |
|---|---|---|
| `CustomerID` | `customer_id` | rename |
| `CustomerDOB` | `birth_date` | parse `d/M/yy`; if year > 2025, subtract 100 years |
| `CustGender` | `gender` | `M`→Male, `F`→Female, else Other |
| `CustLocation` | `location` | rename |
| `CustAccountBalance` | `account_balance` | cast to DECIMAL(18,2) |

Deduplicated by `customer_id` to satisfy the PK constraint. See [transform_customers()](script/transform/transform.py).

#### `transactions` (CSV → DW)
| Source | Target | Transformation |
|---|---|---|
| `TransactionID` | `transaction_id` | rename |
| `CustomerID` | `customer_id` | rename |
| `TransactionDate` | `transaction_date` | parse `d/M/yy`; if year > 2025, subtract 100 years |
| `TransactionTime` | `transaction_time` | left-pad to 6 chars, parse `HHmmss`, format `HH:mm:ss` |
| `TransactionAmount (INR)` | `transaction_amount` | cast to DECIMAL(18,2) |

See [transform_transactions()](script/transform/transform.py).

### 4. Two-digit year rollback (shared helper)
Under PySpark's `LEGACY` parser, `d/M/yy` interprets `21/6/65` as `2065-06-21`.
The helper `_parse_two_digit_year_date()` ([script/transform/transform.py:42](script/transform/transform.py))
parses the date, then subtracts 1200 months (= 100 years) whenever the parsed
year exceeds 2025. Applied to both `birth_date` and `transaction_date`.

### 5. Logging
- Each run creates `script/log/etl_<timestamp>.log` (handler in `logging_helper.py`).
- Logs flow to both file and stdout.
- Log files are gitignored (`script/log/*.log`).

---

## How to run

### 1. Bring containers up
```bash
docker compose build --no-cache
docker compose up -d
```

Three containers will start:
| Container | Role | Host port |
|---|---|---|
| `pyspark_container` | Jupyter + Spark runtime | 8888 (Lab), 4040 (Spark UI) |
| `source_db_container` | Source Postgres (`source` DB) | 5440 |
| `data_warehouse_container` | Warehouse Postgres (`data_warehouse` DB) | 5439 |

### 2. Run the pipeline
```bash
docker exec pyspark_container bash -c \
  "cd /home/jovyan/work && spark-submit --jars /usr/local/spark/jars/postgresql-42.6.0.jar main.py"
```

> **Note:** use `spark-submit`, not `python main.py` — the conda `python` inside
> the container does not have `pyspark` on its path; only `spark-submit`/`pyspark`
> wrappers configure the environment correctly.

### 3. Verify

Tail the latest log:
```bash
tail -n 30 script/log/etl_*.log | tail -n 30
```
Last line should read `=== Pipeline success ===`.

Check warehouse row counts:
```bash
docker exec data_warehouse_container psql -U postgres -d data_warehouse -c "
  SELECT 'education_status' AS tbl, COUNT(*) FROM education_status
  UNION ALL SELECT 'marital_status', COUNT(*) FROM marital_status
  UNION ALL SELECT 'marketing_campaign_deposit', COUNT(*) FROM marketing_campaign_deposit
  UNION ALL SELECT 'customers', COUNT(*) FROM customers
  UNION ALL SELECT 'transactions', COUNT(*) FROM transactions;"
```

### Verified results (latest run)

| Table | Rows |
|---|---|
| `education_status` | 4 |
| `marital_status` | 3 |
| `marketing_campaign_deposit` | 45,211 |
| `customers` | 1,048,567 |
| `transactions` | 1,048,567 |

Transformation spot-checks:
- `balance`: `$` stripped, range −8,019 … 102,127 (INT)
- `duration_in_year`: `floor(duration/365)`, range 0 … 13
- `pdays/previous/poutcome` correctly renamed
- `birth_date` MIN/MAX: 1800-01-01 / 2025-05-06 (no year > 2025)
- `gender`: Male 765,530 / Female 281,936 / Other 1,101
- `transaction_date` MIN/MAX: 2016-08-01 / 2016-10-21
- `transaction_time` formatted as `HH:MM:SS` (e.g. `141103` → `14:11:03`)
- `transaction_amount`, `account_balance` cast to decimal

---

## Project layout
```
.
├── docker-compose.yml          # 3-service stack (pyspark + 2 postgres)
├── Dockerfile                  # pyspark image with JDBC driver baked in
├── driver/postgresql-42.6.0.jar
├── requirements.txt            # python-dotenv, sqlalchemy, psycopg2-binary
├── source/init.sql             # source DB schema + seed data
├── warehouse/init.sql          # warehouse DB schema
├── source-to-target-map.md     # mapping doc (task input)
└── script/                     # pipeline code (see structure above)
```

## Reference style
File structure and ETL conventions follow
[Kurikulum-Sekolah-Pacmann/ecommerce-data-integration](https://github.com/Kurikulum-Sekolah-Pacmann/ecommerce-data-integration/).
