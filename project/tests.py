import os
import sqlalchemy as sa
import pandas as pd

# Import pipeline
import pipeline


def test_load_dataset_csv():
    # Delete old test.csv if existent
    if os.path.exists(f'{pipeline.DATA_DIR_NAME}test.csv'):
        os.remove(f'{pipeline.DATA_DIR_NAME}test.csv')

    # Create simple mock data
    mock_data = ["1,2,3", "Val1,Val2,Val3"]

    # Create mock csv
    with open(f'{pipeline.DATA_DIR_NAME}test.csv', 'w', encoding='utf-8') as file:
        file.write(mock_data[0])
        file.write('\n')
        file.write(mock_data[1])

    # Load dataframe (expected)
    expected_dataframe = pd.read_csv(f'{pipeline.DATA_DIR_NAME}test.csv', sep=',')

    # Run method from pipeline (actual)
    actual_dataframe = pipeline.load_dataset_csv('test.csv', ',')

    # Check if dataframes are equal
    assert expected_dataframe.equals(actual_dataframe)

    # Delete mock csv
    os.remove(f'{pipeline.DATA_DIR_NAME}test.csv')

def test_store_data():
    # Delete old sqlite (if existent)
    if os.path.exists(f"{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}"):
        os.remove(f"{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}")

    # Create simple mock data
    mock_data = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=['a', 'b', 'c'])

    # Expected values
    values_expected = {
        'tables': 1,
        'table_name': 'test',
        'columns': ['a', 'b', 'c'],
        'rows': [(1, 2, 3), (4, 5, 6)]
    }

    # Execute method from pipeline (actual)
    pipeline.store_data(mock_data, 'test')

    # Check if database file are existent
    assert os.path.exists(f'{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}')

    # Open database for detailed checks
    engine_actual = sa.create_engine(f"sqlite:///{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}?charset=utf8")
    inspector_actual = sa.inspect(engine_actual)

    # Check tables
    assert len(inspector_actual.get_table_names()) == values_expected.get('tables')
    assert inspector_actual.get_table_names()[0] == values_expected.get('table_name')

    # Check table 'test'
    # Check if all columns are present
    assert values_expected.get('columns') == [column['name'] for column in inspector_actual.get_columns(values_expected.get('table_name'))]
    
    # Get all rows
    select_query = sa.select('*').select_from(sa.text(values_expected.get('table_name')))
    with engine_actual.connect() as conn:
        result = conn.execute(select_query)
        rows_actual = result.fetchall()

    # Check if all rows are present
    for row in rows_actual:
        assert row in values_expected.get('rows')

    # Delete mock sqlite
    os.remove(f'{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}')

def test_pipeline():
    # Delete old sqlite if existent
    if os.path.exists(f"{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}"):
        os.remove(f"{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}")

    # Execute normal data pipeline
    pipeline.main()

    # Check if database file are existent
    assert os.path.exists(f'{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}')

    # Open resulting SQLite file
    engine = sa.create_engine(f"sqlite:///{pipeline.DATA_DIR_NAME}{pipeline.DATA_FILE_NAME}?charset=utf8")
    inspector = sa.inspect(engine)

    # Get tables from SQLite file
    sql_tables_expected = ["twitch_games", "popular_video_games", "merged"]
    sql_tables = inspector.get_table_names()
    # Check amount of tables
    assert len(sql_tables) == len(sql_tables_expected)
    # Check existance of tables
    for sql_table in sql_tables:
        assert sql_table in sql_tables_expected
