import pandas as pd
from sqlalchemy import create_engine
import requests

def download_csv(url):
    response = requests.get(url)
    return response.content.decode('utf-8-sig')

def process_data(csv_content):
    df = pd.read_csv(pd.compat.StringIO(csv_content), skiprows=6, skipfooter=4, engine='python')
    df = df[['A', 'B', 'C', 'M', 'W', 'AG', 'AQ', 'BA', 'BK', 'BU']]
    df.columns = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
    df = df[df['CIN'].astype(str).str.len() == 5]
    df[['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']] = df[['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']].apply(pd.to_numeric, errors='coerce')
    df.dropna(inplace=True)
    return df

def save_to_database(df, db_name):
    engine = create_engine(f'sqlite:///{db_name}')
    df.to_sql('cars', engine, if_exists='replace', index=False)
    
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
csv_content = download_csv(url)
df = process_data(csv_content)
save_to_database(df, 'cars.sqlite')
