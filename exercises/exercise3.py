import pandas as pd
import sqlalchemy
import requests
from io import StringIO


def download_csv(url):
    response = requests.get(url)
    return response.content.decode('ISO-8859-1')


def process_data(csv_content):
    df = pd.read_csv(StringIO(csv_content), sep=';', encoding='iso-8859-1', skiprows=6, skipfooter=4, engine='python')

    df = df.iloc[:, [0, 1, 2, 12, 22, 32, 42, 52, 62, 72]]
    df.columns = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

    # Daten validieren
    df['CIN'] = df['CIN'].astype(str).str.zfill(5)
    numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col] > 0]

    return df


def save_to_database(df, db_name, table_name):
    engine = sqlalchemy.create_engine(f'sqlite:///{db_name}')
    column_types = {
        'date': sqlalchemy.String(20),
        'CIN': sqlalchemy.String(255),
        'name': sqlalchemy.String(255),
        'petrol': sqlalchemy.Integer,
        'diesel': sqlalchemy.Integer,
        'gas': sqlalchemy.Integer,
        'electro': sqlalchemy.Integer,
        'hybrid': sqlalchemy.Integer,
        'plugInHybrid': sqlalchemy.Integer,
        'others': sqlalchemy.Integer
    }
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, dtype=column_types)


    url = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
    csv_content = download_csv(url)
    df = process_data(csv_content)
    save_to_database(df, 'cars.sqlite', 'cars')