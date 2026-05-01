# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** Thami Goqo  
**Date:** 2026-05-01  
**Status:** Final  

---

## Context

Stage 3 required extending the existing batch data pipeline with a streaming-style ingestion layer for micro-batch JSONL files delivered into `/data/stream/` while the container is running. The mobile product team required two near-real-time output tables: `current_balances`, which maintains one current balance row per account, and `recent_transactions`, which maintains the most recent 50 transactions per account. The stream processor also needed to terminate on its own after a quiet period so the container would not run indefinitely.

Before Stage 3, the pipeline already implemented a medallion architecture using `ingest.py`, `transform.py`, and `provision.py`. The batch path produced Bronze, Silver, and Gold Delta tables, including `dim_accounts`, `dim_customers`, `fact_transactions`, and `dq_report.json`. Spark configuration was centralised in `spark.py`, and file paths were read from `config/pipeline_config.yaml`. That existing structure gave the streaming extension a stable foundation, but Stage 3 introduced a new requirement: maintaining incremental account state instead of only overwriting batch outputs.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

The Stage 1 and Stage 2 architecture helped because the project was already organised into clear pipeline modules. `ingest.py` handled raw input ingestion, `transform.py` handled standardisation and DQ logic, and `provision.py` handled Gold-layer modelling. Because these responsibilities were separated, I could add `stream_ingest.py` as a separate Stage 3 extension without rewriting the existing batch flow. Centralising Spark setup in `spark.py` also helped, because the stream processor reused the same Delta-enabled Spark session and resource settings as the batch pipeline.

The main friction was that the original pipeline was designed around full-table batch overwrite patterns, while Stage 3 required incremental state updates. `current_balances` needed one row per account that could be updated as new transactions arrived, while `recent_transactions` needed to preserve only the most recent 50 transactions per account. That required adding merge/upsert logic and a rolling window pattern, which were not needed in the earlier batch-only stages. Most Stage 1/2 code survived intact; Stage 3 was mainly an extension through a new `stream_ingest.py` module and a small update to `run_all.py`.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

In hindsight, I would have designed the pipeline entry point with explicit execution modes from the start, such as `batch`, `stream`, and `all`. The original `run_all.py` was simple and worked well for Stage 1/2, but Stage 3 introduced a polling loop that behaves differently from a normal batch job. A mode-based entry point would have made it clearer when the pipeline should run only Bronze/Silver/Gold versus when it should also wait for `/data/stream/` micro-batches.

I would also have separated schema definitions into a shared module such as `pipeline/schemas.py`. In the current implementation, schema handling is spread across `transform.py`, `provision.py`, and `stream_ingest.py`. This worked, but adding `current_balances` and `recent_transactions` meant repeating some column casting, timestamp parsing, and safe-column logic. If schemas and common transformations had been centralised earlier, Stage 3 would have required less tracing across files and fewer defensive checks in the stream processor.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If I had known Stage 3 was coming from the beginning, I would have designed the pipeline around two ingestion interfaces: one for static batch files and one for micro-batch stream files. The batch interface would read `/data/input/`, while the stream interface would poll `/data/stream/`. Both would share the same Spark session, date parsing, amount casting, currency normalisation, and Delta writing utilities.

I would also have designed state management as a first-class part of the architecture. `current_balances` is not just another output table; it represents account state that changes over time. I would still use Delta tables because they support reliable table updates, but I would define a reusable merge/upsert helper instead of embedding that logic only inside `stream_ingest.py`. For `recent_transactions`, I would define a reusable windowing function to enforce the latest-50-transactions rule after every micro-batch. Finally, I would keep `gold/` and `stream_gold/` separate, but use shared schema and validation utilities so batch and streaming outputs follow the same engineering standards.