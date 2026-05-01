# Nedbank Data Engineering Challenge — Stage 3 Submission

## Overview

This repository contains a completed Data Engineering challenge solution built with PySpark, Delta Lake, and Docker.

The pipeline implements a full medallion architecture:

1. **Bronze layer** — raw ingestion of source files with ingestion timestamps.
2. **Silver layer** — type standardisation, date normalisation, deduplication, schema enforcement, and data quality handling.
3. **Gold layer** — dimensional modelling for analytics and validation queries.
4. **Stage 3 streaming extension** — micro-batch JSONL stream processing for current balances and recent transactions.

The solution is designed to run inside the required Docker interface and write Delta Parquet outputs to `/data/output`.



## Repository Structure

```text
.
├── Dockerfile
├── requirements.txt
├── README.md
├── adr/
│   └── stage3_adr.md
├── config/
│   ├── pipeline_config.yaml
│   └── dq_rules.yaml
├── jars/
│   ├── delta-spark_2.12-3.1.0.jar
│   ├── delta-storage-3.1.0.jar
│   └── antlr4-runtime-4.9.3.jar
├── pipeline/
│   ├── __init__.py
│   ├── config_loader.py
│   ├── spark.py
│   ├── ingest.py
│   ├── transform.py
│   ├── provision.py
│   ├── stream_ingest.py
│   └── run_all.py
└── stream/
    └── README.md
```

---
** The batch pipeline processes the mounted input files:

/data/input/accounts.csv
/data/input/customers.csv
/data/input/transactions.jsonl

## Bronze Layer
pipeline/ingest.py reads all three source datasets and writes raw Delta tables with an added ingestion_timestamp.

## Output:
/data/output/bronze/accounts/
/data/output/bronze/customers/
/data/output/bronze/transactions/


## Silver Layer
pipeline/transform.py reads Bronze tables and applies:
Deduplication on natural keys
Type casting
Date standardisation
Currency normalisation
Safe handling of missing merchant_subcategory
Data quality flagging
Exclusion/quarantine rules for invalid records

## Output:
/data/output/silver/accounts/
/data/output/silver/customers/
/data/output/silver/transactions/

## Gold Layer
pipeline/provision.py provisions the dimensional model:
/data/output/gold/dim_accounts/
/data/output/gold/dim_customers/
/data/output/gold/fact_transactions/

**The Gold layer includes stable surrogate keys, resolved account/customer relationships, derived age_band, and a Stage 2-compatible fact_transactions schema**

**Data Quality**
Data quality rules are externalised in:
config/dq_rules.yaml

## The pipeline detects and handles the six required DQ issue categories:
1.DUPLICATE_DEDUPED
2.ORPHANED_ACCOUNT
3.TYPE_MISMATCH
4.DATE_FORMAT
5.CURRENCY_VARIANT
6.NULL_REQUIRED

The DQ report is written to:
/data/output/dq_report.json

## The report includes:
Source record counts
Encountered data quality issues
Handling actions
Gold layer record counts
Execution duration

## stage 3 Streaming Extension
**The Stage 3 extension processes micro-batch JSONL transaction files from:**
/data/stream/

**pipeline/stream_ingest.py processes stream files in filename order and writes two Delta tables:**
/data/output/stream_gold/current_balances/
/data/output/stream_gold/recent_transactions/

-current_balances
Maintains one row per account_id
Fields:
account_id
current_balance
last_transaction_timestamp
updated_at

-recent_transactions
Maintains the 50 most recent transactions per account.
Fields:
account_id
transaction_id
transaction_timestamp
amount
transaction_type
channel
updated_at
**The streaming processor tracks processed files and terminates after a quiet period with no new files.**

## Docker Build
Build the image:
docker build -t nedbank-pipeline .

## Docker Run
Run using the challenge Docker contract:
docker run --rm \
  -v /path/to/data:/data \
  --memory=2g \
  --cpus="2" \
  nedbank-pipeline

**On Windows PowerShell:**
docker run --rm `
  -v ${PWD}/data:/data `
  --memory=2g `
  --cpus="2" `
  nedbank-pipeline

**Expected Outputs:**
```text
/data/output/
├── bronze/
│   ├── accounts/
│   ├── customers/
│   └── transactions/
├── silver/
│   ├── accounts/
│   ├── customers/
│   └── transactions/
├── gold/
│   ├── dim_accounts/
│   ├── dim_customers/
│   └── fact_transactions/
├── stream_gold/
│   ├── current_balances/
│   └── recent_transactions/
└── dq_report.json
  ```

---
## Notes
Source data is not committed to this repository.
Generated output data is not committed to this repository.
The scoring system mounts /data/input, /data/config, and /data/stream at runtime.
The pipeline is non-interactive and exits with code 0 on successful completion.
Bronze, Silver, Gold, and Stream Gold outputs are distinct and separable.
AI assisted engineering.

