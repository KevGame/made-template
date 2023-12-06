import pandas as pd
import sqlalchemy as sa

CSV_URL = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"

def letters_to_numbers(column_chars: list[str]) -> list[int]:
    # Create lists for capital chars and numbers
    capital_chars = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    chars_nums = [i for i in range(0, len(capital_chars))]

    # Create mapping dictionary
    letters_mapping = {k: v for k, v in zip(capital_chars, chars_nums)}

    # Iterate through the list of characters and map them to numbers
    column_nums = [None] * len(column_chars)
    for col_idx in range(0, len(column_chars)):
        # Reset number for new iteration
        column_number = 0

        # read single column as list
        single_col = list(column_chars[col_idx])

        # Check if there are two letters in the column
        if len(list(column_chars[col_idx])) == 2:
            # Read first letter and add "cycles" based on mapping [e. g. B = 2 * len(capital_chars)]
            column_number += (1 + letters_mapping[single_col[0]]) * len(capital_chars)

        # Map last letter to number
        column_number += letters_mapping[single_col[len(single_col) - 1]]
        
        # After calculation write the final column number in the list
        column_nums[col_idx] = column_number 

    # After all conversion return the column list represented as numbers
    return column_nums

def load_data() -> pd.DataFrame:
    # Columns that should be used in relation to the task
    # To prevent manual conversion between letters and numbers use helper function
    columns_usage = letters_to_numbers(['A', 'B', 'C', 'M', 'W', 'AG', 'AQ', 'BA', 'BK', 'BU'])

    # Names and types for columns according to the task
    column_names = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
    column_types = [str, str, str, 'Int64', 'Int64', 'Int64', 'Int64', 'Int64', 'Int64', 'Int64']
    column_dtypes = {k: v for k, v in zip(column_names, column_types)}

    # Load external CSV file with options and skip first 6 lines and last 4 lines
    csv_data = pd.read_csv(
        CSV_URL,
        sep=';',
        header=0,
        names=column_names,
        usecols=columns_usage,
        dtype=column_dtypes,
        skiprows=6,
        skipfooter=4,
        na_values=['-'],
        engine='python',
        encoding='ISO-8859-1',
        on_bad_lines='skip'
    )

    return csv_data

def validate_data(csv_data: pd.DataFrame) -> pd.DataFrame:
    # Drop invalid values
    csv_data.dropna(inplace=True)

    # Drop invalid CINs (string with 5 characters, can have leading 0)
    csv_data = csv_data[csv_data['CIN'].str.match(r'^[0-9]{5}$')]

    # Check if other columns have values greater 0
    csv_data = csv_data[csv_data['petrol'] > 0]
    csv_data = csv_data[csv_data['diesel'] > 0]
    csv_data = csv_data[csv_data['gas'] > 0]
    csv_data = csv_data[csv_data['electro'] > 0]
    csv_data = csv_data[csv_data['hybrid'] > 0]
    csv_data = csv_data[csv_data['plugInHybrid'] > 0]
    csv_data = csv_data[csv_data['others'] > 0]

    return csv_data

def create_sqlite(csv: pd.DataFrame) -> None:
    # Create engine with name airports for communication
    engine = sa.create_engine("sqlite:///cars.sqlite")

    # Create type mapping
    column_dtypes = {
        'date': sa.VARCHAR(10),
        'CIN': sa.VARCHAR(5),
        'name': sa.TEXT,
        'petrol': sa.INTEGER,
        'diesel': sa.INTEGER,
        'gas': sa.INTEGER,
        'electro': sa.INTEGER,
        'hybrid': sa.INTEGER,
        'plugInHybrid': sa.INTEGER,
        'others': sa.INTEGER
    }

    # Create SQL-File with the specified mappings
    csv.to_sql("cars", engine, index=False, dtype=column_dtypes, if_exists='replace')

def main():
    csv_data = load_data()
    csv_data = validate_data(csv_data)
    create_sqlite(csv_data)

if __name__ == "__main__":
    main()