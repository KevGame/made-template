import pandas as pd
import sqlalchemy as sa
import urllib.request as request
from zipfile import ZipFile
import os

DATA_URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
FILE_NAME = "stops.txt"

def load_data() -> pd.DataFrame:
    # Download external ZIP file
    request.urlretrieve(DATA_URL, 'GTFS.zip')

    # Extract stops.txt from ZIP file
    with ZipFile('GTFS.zip', 'r') as zip_file:
        zip_file.extract(FILE_NAME)

    # Delete ZIP file
    os.remove('GTFS.zip')

    # Names and types for columns according to the task
    column_names = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
    column_types = ['Int32', str, 'Float64', 'Float64', 'Int32']
    column_dtypes = {k: v for k, v in zip(column_names, column_types)}

    # Load stops.txt as CSV and select columns with data types
    csv_data = pd.read_csv(
        FILE_NAME,
        sep=',',
        header=0,
        usecols=column_names,
        dtype=column_dtypes,
        encoding='UTF-8',
        on_bad_lines='skip'
    )

    return csv_data

def filter_data(csv_data: pd.DataFrame) -> pd.DataFrame:
    # Select zone_id that are 2001
    csv_data = csv_data[csv_data['zone_id'] == 2001]

    return csv_data

def validate_data(csv_data: pd.DataFrame) -> pd.DataFrame:
    # Drop rows with invalid and missing data
    csv_data = csv_data.dropna()

    # Check if stop_lat/stop_lon are geographical coordinates (between -90 and 90, including upper and lower bounds)
    csv_data = csv_data[csv_data['stop_lat'].between(-90, 90, inclusive='both')]
    csv_data = csv_data[csv_data['stop_lon'].between(-90, 90, inclusive='both')]

    return csv_data

def create_sqlite(csv: pd.DataFrame) -> None:
    # Create engine with name gtfs for communication
    engine = sa.create_engine("sqlite:///gtfs.sqlite")

    # Create type mapping
    column_dtypes = {
        'stop_id': sa.INTEGER,
        'stop_name': sa.TEXT,
        'stop_lat': sa.FLOAT,
        'stop_lon': sa.FLOAT,
        'zone_id': sa.INTEGER
    }

    # Create SQL-File with the specified mappings
    csv.to_sql('stops', engine, index=False, dtype=column_dtypes, if_exists='replace')

def main():
    csv_data = load_data()
    csv_data = filter_data(csv_data)
    csv_data = validate_data(csv_data)
    create_sqlite(csv_data)

if __name__ == "__main__":
    main()
