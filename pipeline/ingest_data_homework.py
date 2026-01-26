#!/usr/bin/env python
# coding: utf-8

"""
Ingest data into PostgreSQL.
Supports:
- CSV / CSV.GZ via pandas chunks
- Parquet via pyarrow batches

This is a homework-ready ingestion script (step 3).
"""

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.parquet as pq

# -----------------------------
# Schema helpers (NYC Taxi-like)
# -----------------------------
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


# -----------------------------
# Internal helpers
# -----------------------------

def _create_table(df: pd.DataFrame, engine, table: str) -> None:
    """Create (replace) table schema without inserting data."""
    df.head(0).to_sql(name=table, con=engine, if_exists="replace", index=False)
    print(f"Table {table} created")


# -----------------------------
# Ingestion implementations
# -----------------------------

def ingest_csv(url_or_path: str, engine, table: str, chunksize: int) -> None:
    df_iter = pd.read_csv(
        url_or_path,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    first_chunk = next(df_iter)
    _create_table(first_chunk, engine, table)

    first_chunk.to_sql(name=table, con=engine, if_exists="append", index=False)
    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter, desc="Ingesting CSV"):
        df_chunk.to_sql(name=table, con=engine, if_exists="append", index=False)

    print(f"done ingesting to {table}")


def ingest_parquet(path: str, engine, table: str, chunksize: int) -> None:
    parquet_file = pq.ParquetFile(path)

    first = True
    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize), desc="Ingesting Parquet"):
        df_chunk = batch.to_pandas()

        # Ensure datetime columns
        for col in PARSE_DATES:
            if col in df_chunk.columns and not pd.api.types.is_datetime64_any_dtype(df_chunk[col]):
                df_chunk[col] = pd.to_datetime(df_chunk[col])

        if first:
            _create_table(df_chunk, engine, table)
            first = False

        df_chunk.to_sql(name=table, con=engine, if_exists="append", index=False)

    print(f"done ingesting to {table}")


# -----------------------------
# CLI
# -----------------------------

@click.command()
@click.option("--pg-user", default="root", show_default=True)
@click.option("--pg-pass", default="root", show_default=True)
@click.option("--pg-host", default="localhost", show_default=True)
@click.option("--pg-port", default=5432, type=int, show_default=True)
@click.option("--pg-db", default="ny_taxi", show_default=True)
@click.option("--target-table", required=True, help="Target table name in Postgres")
@click.option("--chunksize", default=100000, type=int, show_default=True)
@click.option("--csv-url", default=None, help="CSV/CSV.GZ URL or local path")
@click.option("--parquet-path", default=None, help="Local Parquet file path")
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, csv_url, parquet_path):
    if (csv_url is None) == (parquet_path is None):
        raise click.UsageError("Provide exactly one of --csv-url or --parquet-path")

    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    if csv_url:
        ingest_csv(csv_url, engine, target_table, chunksize)
    else:
        ingest_parquet(parquet_path, engine, target_table, chunksize)


if __name__ == "__main__":
    main()
