import sqlite3
import pytest
import os


# The URL: 'https://microdata.worldbank.org/index.php/catalog/4509/download/67079' for the food_price_inflation-Dataset
# has become unavailable during my project work, for the GitHub-Workflow I have disabled downloading and testing it

@pytest.fixture
def db_cursor():
    db_connection = sqlite3.connect("../data/zylesto.sqlite")
    cursor = db_connection.cursor()
    yield cursor
    db_connection.close()


@pytest.fixture
def db_connection():
    db_path = "../data/zylesto.sqlite"
    if not os.path.isfile(db_path):
        pytest.fail(f"Error: The specified path '{db_path}' does not exist or is not a regular file path.")
    try:
        connection = sqlite3.connect(db_path)
        yield connection
    finally:
        connection.close()


def test_valid_sqlite_database(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    assert len(tables) > 0, "The database does not contain any tables, hence it is not a valid SQLite database."


def test_tables_exist(db_cursor):
    db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = db_cursor.fetchall()
    # assert ("food_price_inflation",) in tables
    assert ("temperature",) in tables


def extract_column_info(columns):
    return [(name, type_) for _, name, type_, _, _, _ in columns]


@pytest.mark.parametrize("table_name, expected_columns", [
    # ("food_price_inflation", [
    #     ("Open", "FLOAT"),
    #     ("High", "FLOAT"),
    #     ("Low", "FLOAT"),
    #     ("Close", "FLOAT"),
    #     ("Inflation", "FLOAT"),
    #     ("country", "TEXT"),
    #     ("ISO3", "TEXT"),
    #     ("date", "TEXT"),
    # ]),
    ("temperature", [
        ("Area Code", "BIGINT"),
        ("Area Code (M49)", "TEXT"),
        ("Area", "TEXT"),
        ("Months Code", "BIGINT"),
        ("Months", "TEXT"),
        ("Element", "TEXT"),
        ("Unit", "TEXT"),
    ])
])
def test_table_columns(db_cursor, table_name, expected_columns):
    db_cursor.execute(f"PRAGMA table_info({table_name});")
    columns = db_cursor.fetchall()
    extracted_columns = extract_column_info(columns)
    for column, data_type in expected_columns:
        assert (column, data_type) in extracted_columns


@pytest.mark.parametrize("table_name", ["temperature"  # , "food_price_inflation"
                                        ])
def test_table_data(db_cursor, table_name):
    db_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = db_cursor.fetchone()[0]
    assert count > 0


@pytest.mark.parametrize("table_name, column_name, min_value, max_value", [
    ("temperature", "Y1961", -50, 50),
    # ("food_price_inflation", "Inflation", -100, 1000)
])
def test_data_validity(db_cursor, table_name, column_name, min_value, max_value):
    query = f"SELECT {column_name} FROM {table_name} WHERE {column_name} NOT BETWEEN ? AND ?;"
    db_cursor.execute(query, (min_value, max_value))
    invalid_values = db_cursor.fetchall()
    assert len(invalid_values) == 0, f"Found invalid values in column {column_name} of table {table_name}"


@pytest.mark.parametrize("table_name, year_column_prefix, start_year, end_year", [
    ("temperature", "Y", 1961, 2020),
    # ("food_price_inflation", "Y", 2000, 2020)
])
def test_time_series_consistency(db_cursor, table_name, year_column_prefix, start_year, end_year):
    for year in range(start_year, end_year + 1):
        column_name = f"{year_column_prefix}{year}"
        db_cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL;")
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found NULL values in column {column_name} of table {table_name}"
