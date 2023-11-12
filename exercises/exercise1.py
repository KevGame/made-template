import pandas as pd
import sqlalchemy as sa

CSV_URL = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

def load_data() -> pd.DataFrame:
    # Load external CSV file
    csv_data = pd.read_csv(CSV_URL, sep=";")
    #print(csv_data)
    return csv_data

def create_sqlite(csv: pd.DataFrame):
    # Create engine with name airports for communication
    engine = sa.create_engine("sqlite:///airports.sqlite")

    # Create type mapping
    type_mapping = {
        "column_1": sa.INTEGER,
        "column_2": sa.TEXT,
        "column_3": sa.TEXT,
        "column_4": sa.TEXT,
        "column_5": sa.VARCHAR(3),
        "column_6": sa.VARCHAR(4),
        "column_7": sa.FLOAT,
        "column_8": sa.FLOAT,
        "column_9": sa.INTEGER,
        "column_10": sa.FLOAT,
        "column_11": sa.CHAR(1),
        "column_12": sa.TEXT,
        "geo_punkt": sa.TEXT
    }

    # Create SQL-File with the specified mappings
    csv.to_sql("airports", engine, index=False, dtype=type_mapping)

def main():
    csv_data = load_data()
    create_sqlite(csv_data)

if __name__ == "__main__":
    main()