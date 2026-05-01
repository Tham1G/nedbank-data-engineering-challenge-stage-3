"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""
"""
Pipeline entry point.
Runs ingestion only for now.
"""

from ingest import run_ingestion
from transform import run_transformation
from provision import run_provisioning
from stream_ingest import run_stream_ingestion


def main():
    print("Pipeline started")

    run_ingestion()
    run_transformation()
    run_provisioning()
    run_stream_ingestion()

    print("Pipeline completed successfully")


if __name__ == "__main__":
    main()
