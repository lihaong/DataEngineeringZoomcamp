#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import requests


def download_file(url, filename):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check for HTTP errors
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except requests.RequestException as e:
        print(f"Error downloading file: {e}")
        raise


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url
    csv_name = "output.csv"

    # Download the data
    print("Downloading data...")
    download_file(url, csv_name)

    # Create database engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    try:
        df_iter = pd.read_csv(
            csv_name,
            compression="gzip",
            iterator=True,
            chunksize=100000,
        )
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Process first chunk
    try:
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(
            df.tpep_pickup_datetime, errors="coerce"
        )
        df.tpep_dropoff_datetime = pd.to_datetime(
            df.tpep_dropoff_datetime, errors="coerce"
        )
        df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")
        df.to_sql(name=table, con=engine, if_exists="append")
    except Exception as e:
        print(f"Error processing initial chunk: {e}")
        return

    # Process remaining chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(
                df.tpep_pickup_datetime, errors="coerce"
            )
            df.tpep_dropoff_datetime = pd.to_datetime(
                df.tpep_dropoff_datetime, errors="coerce"
            )
            df.to_sql(name=table, con=engine, if_exists="append")
            t_end = time()
            print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")
        except StopIteration:
            print("All chunks processed.")
            break
        except Exception as e:
            print(f"Error processing chunk: {e}")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest data into the postgres database"
    )
    parser.add_argument(
        "--user", type=str, required=True, help="User name for postgres"
    )
    parser.add_argument(
        "--password", type=str, required=True, help="Password for postgres"
    )
    parser.add_argument("--host", type=str, required=True, help="Host for postgres")
    parser.add_argument("--port", type=str, required=True, help="Port for postgres")
    parser.add_argument(
        "--database", type=str, required=True, help="Database name for postgres"
    )
    parser.add_argument(
        "--table", type=str, required=True, help="Table name for postgres"
    )
    parser.add_argument("--url", type=str, required=True, help="URL for data")

    args = parser.parse_args()
    main(args)
