#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import urllib.request
import tempfile
import os


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading parquet')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    """Ingest green taxi data into PostgreSQL database."""
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f"Downloading green taxi data from {url}...")
    
    # Download the parquet file to a temporary location
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        with urllib.request.urlopen(url) as response:
            tmp_file.write(response.read())
    
    try:
        print(f"Reading parquet file...")
        df = pd.read_parquet(tmp_path)
        
        print(f"Ingesting {len(df)} green taxi records into {target_table} table...")
        
        # Write to PostgreSQL
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists='replace',
            index=False
        )
        
        print(f"Successfully loaded {len(df)} green taxi records into {target_table}")
    
    finally:
        # Clean up temporary file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


if __name__ == '__main__':
    run()
