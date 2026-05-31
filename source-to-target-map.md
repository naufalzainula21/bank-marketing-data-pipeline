
## Column Mapping

### Education Status
source : table education_status

target : table education_status

| Source Column   | Target Column   | Transformation                                   |
|----------------|----------------|---------------------------------------------------------|
| `education_id` | `education_id` | -                       |
| `value`        | `value`        | - |
| `created_at`   | `created_at`   | -           |
| `updated_at`   | `updated_at`   | -          |


### Marital Status
source : table marital_status

target : table marital_status
| Source Column   | Target Column   | Transformation |
|----------------|----------------|---------------|
| `marital_id`   | `marital_id`   | - |
| `value`        | `value`        | - |
| `created_at`   | `created_at`   | - |
| `updated_at`   | `updated_at`   | - |


### Marketing Campaign for Deposit
source : table marketing_campaign_deposit

target : table marketing_campaign_deposit
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|-----------------------------|----------------------------------------------------|
| `loan_data_id`             | `loan_data_id`              | - |
| `age`                      | `age`                       | - |
| `job`                      | `job`                       | - |
| `marital_id`               | `marital_id`                | - |
| `education_id`             | `education_id`              | - |
| `"default"`                | `"default"`                 | - |
| `balance`                  | `balance`                   | Remove `$` sign and convert to `INT` |
| `housing`                  | `housing`                   | - |
| `loan`                     | `loan`                      | - |
| `contact`                  | `contact`                   | - |
| `"day"`                    | `"day"`                     | - |
| `"month"`                  | `"month"`                   | - |
| `duration`                 | `duration`                  | - |
| `duration`                 | `duration_in_year`          | duration divide by `365`, round down, and cast to `INT` |
| `campaign`                 | `campaign`                  | - |
| `pdays`                    | `days_since_last_campaign`  | Rename column |
| `previous`                 | `previous_campaign_contacts`| Rename column |
| `poutcome`                 | `previous_campaign_outcome` | Rename column |
| `subscribed_deposit`       | `subscribed_deposit`        | - |
| `created_at`               | `created_at`                | - |
| `updated_at`               | `updated_at`                | - |

### Customers
source : file new_bank_transaction.csv

target : table customers

| Source Column          | Target Column      | Transformation                                      |
|------------------------|-------------------|----------------------------------------------------|
| `CustomerID`          | `customer_id`      | Rename column |
| `CustomerDOB`         | `birth_date`       | Convert to `DATE` format (`d/M/yy`), adjust years if > 2025 |
| `CustGender`          | `gender`           | Rename column; Map `M` → `Male`, `F` → `Female`, others → `Other` |
| `CustLocation`        | `location`         | Rename column |
| `CustAccountBalance`  | `account_balance`  | Rename column, cast to decimal number |

### Transactions
source : file new_bank_transaction.csv

target : table transactions

| Source Column                 | Target Column      | Transformation                                                   |
|--------------------------------|-------------------|-----------------------------------------------------------------|
| `TransactionID`               | `transaction_id`  | Rename column |
| `CustomerID`                  | `customer_id`     | Rename column |
| `TransactionDate`             | `transaction_date` | Convert to `DATE` format (`d/M/yy`), adjust years if > 2025 |
| `TransactionTime`             | `transaction_time` | Convert to `HH:MM:SS` format |
| `TransactionAmount (INR)`     | `transaction_amount` | Rename column, cast to decimal number |
