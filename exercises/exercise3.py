import pandas as pd
from sqlalchemy import create_engine, Integer, String
from io import StringIO
import urllib.request


def download_csv(url):
    response = urllib.request.urlopen(url)
    return response.read().decode('ISO-8859-1')


def process_data(csv_content):
    df = pd.read_csv(StringIO(csv_content), sep=';', encoding='iso-8859-1', skiprows=6, skipfooter=4, engine='python')

    df = df.iloc[:, [0, 1, 2, 12, 22, 32, 42, 52, 62, 72]]
    df.columns = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

    df['CIN'] = df['CIN'].astype(str).str.zfill(5)
    numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col] > 0]

    return df


def save_to_database(df):
    engine = create_engine('sqlite:///cars.sqlite')
    column_types = {
        'date': String,
        'CIN': String,
        'name': String,
        'petrol': Integer,
        'diesel': Integer,
        'gas': Integer,
        'electro': Integer,
        'hybrid': Integer,
        'plugInHybrid': Integer,
        'others': Integer
    }

    df.to_sql(con=engine, name='cars', if_exists='replace', index=False, dtype=column_types)


url = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
csv_content = download_csv(url)

df = process_data(csv_content)

save_to_database(df)
