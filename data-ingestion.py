# Import Libraries
import logging
import os

import click
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tqdm.auto import tqdm

load_dotenv()

dtype = {
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
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_data(url: str,
                engine,
                target_table: str,
                chunksize: int = 100000) -> None:
    """
    Ingest data from a CSV or Parquet file to a Postgres database in batches.
    
    Parameters:
    url (str): The URL of the file (CSV or Parquet).
    engine: The SQLAlchemy engine object for the database connection.
    target_table (str): The name of the target table in the database.
    chunksize (int): The number of rows to read at a time (default is 100000).
    
    Returns:
    None
    
    Raises:
    ValueError: If the file format is unsupported (not CSV or Parquet).
    """
    
    df_iter = None
    # Determine file format
    if url.lower().endswith(('.csv', '.csv.gz')):
        # Read the CSV data in batches
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )
    elif url.lower().endswith(('.parquet', '.pq')):
        # Read the Parquet data with memory mapping for efficiency
        df = pd.read_parquet(url, engine='pyarrow', memory_map=True)
        # Split into chunks
        df_iter = [df.iloc[i:i+chunksize] for i in range(0, len(df), chunksize)]
    else:
        raise ValueError("Unsupported file format. Only CSV and Parquet are supported.")
    
    header = True
    for df_chunk in tqdm(df_iter):
        # First, create the structure of the table without any data
        # If the table already exists, replace it (delete the existing table and create a new one).
        if header:
            # Create only the table schema
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            header = False 
            logging.info("Table created")
        # Insert data in batches.
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        logging.info(f"Inserted: {len(df_chunk)} rows")
    logging.info(f"Data ingestion completed. Target table: {target_table}")

@click.command()
@click.option('--target_table', default='yellow_taxi_data_01_2026', help='Target database table name')
@click.option('--url', required=True, help='URL or path to the data file (CSV or Parquet)')
@click.option('--chunksize', default=100000, type=int, help='Chunk size (rows per batch)')
def main(target_table, url, chunksize):
    """
    Ingest taxi trip data from a CSV or Parquet file into a PostgreSQL database.
    
    Parameters:
    target_table (str): Target table name (default: yellow_taxi_data_01_2026)
    url (str): URL or path to the trip data file (required)
    chunksize (int): Number of rows to process per batch (default: 100000)
    """
    pg_user = os.getenv('POSTGRES_USER')
    pg_pass = os.getenv('POSTGRES_PASSWORD')
    pg_host = os.getenv('POSTGRES_HOST')
    pg_port = os.getenv('POSTGRES_PORT')
    pg_db = os.getenv('POSTGRES_DB')

    missing = [name for name, value in [
        ('POSTGRES_USER', pg_user),
        ('POSTGRES_PASSWORD', pg_pass),
        ('POSTGRES_HOST', pg_host),
        ('POSTGRES_PORT', pg_port),
        ('POSTGRES_DB', pg_db),
    ] if not value]
    if missing:
        raise click.ClickException(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    logging.info('Starting taxi trip data ingestion...')
    # Create Database Connection with fast executemany enabled
    connection_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/ny_taxi_db')
    
    # Read the data in batches
    ingest_data(
        url=url,
        engine=connection_engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == "__main__":
    main()