import sqlite3
import pytest

@pytest.fixture
def db_cursor():
    db_connection = sqlite3.connect("../data/zylesto.sqlite")
    cursor = db_connection.cursor()
    yield cursor
    db_connection.close()

def test_tables_exist(db_cursor):
    db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = db_cursor.fetchall()
    assert ("food_price_inflation",) in tables
    assert ("temperature",) in tables

def extract_column_info(columns):
    return [(name, type_) for _, name, type_, _, _, _ in columns]

@pytest.mark.parametrize("table_name, expected_columns", [
    ("food_price_inflation", [
        ("Open", "FLOAT"),
        ("High", "FLOAT"),
        ("Low", "FLOAT"),
        ("Close", "FLOAT"),
        ("Inflation", "FLOAT"),
        ("country", "TEXT"),
        ("ISO3", "TEXT"),
        ("date", "TEXT"),
    ]),
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

@pytest.mark.parametrize("table_name", ["food_price_inflation", "temperature"])
def test_table_data(db_cursor, table_name):
    db_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = db_cursor.fetchone()[0]
    assert count > 0
