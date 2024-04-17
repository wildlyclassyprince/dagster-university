import requests
import os
import duckdb

from . import constants
from dagster import asset


@asset
def taxi_trips_file():
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYV Open Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet'
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), 'wb') as output_file:
        output_file.write(raw_trips.content)

@asset
def taxi_zones_file():
    """
    The raw parquet files for the taxi zones dataset. Sourced from the NYU Open Data portal.
    """
    raw_taxi_zones = requests.get('https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD')

    with open(constants.TAXI_ZONES_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_taxi_zones.content)

@asset(deps=['taxi_trips_file'])
def taxi_trips():
    """
    The raw taxi trips dataset, loaded into a DuckDB database.
    """
    sql_query = """
        create or replace table trips as (
            select
                VendorID as vendor_id,
                PULocationID as pickup_zone_id,
                DOLocationID as dropoff_zone_id,
                RatecodeID as rate_code_id,
                payment_type as payment_type,
                tpep_dropoff_datetime as pickup_datetime,
                trip_distance as trip_distance,
                passenger_count as passenger_count,
                total_amount as total_amount
            from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    conn = duckdb.connect(os.getenv('DUCKDB_DATABASE'))
    conn.execute(sql_query)

@asset(deps=['taxi_zones_file'])
def taxi_zones():
    """
    The raw taxi zones dataset, loaded into a DuckDB database.
    """
    sql_query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone as zone,
                borough as borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    conn = duckdb.connect(os.getenv('DUCKDB_DATABASE'))
    conn.execute(sql_query)
