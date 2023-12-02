import pandas as pd
import requests
import zipfile
from io import BytesIO


def download_zip_content(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    with zipfile.ZipFile(BytesIO(response.content)) as zfile:
        file_names = zfile.namelist()
        with zfile.open(file_names[0]) as zd:
            df = pd.read_csv(zd, encoding='latin1')
            return df.dropna()


def download_csv(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    df = pd.read_csv(BytesIO(response.content), encoding='latin1')
    return df.dropna()


def save_to_sqlite(df, table_name, db_url='sqlite:///../data/zylesto.sqlite'):
    df.to_sql(table_name, db_url, if_exists='replace', index=False)


url1 = 'https://fenixservices.fao.org/faostat/static/bulkdownloads/Environment_Temperature_change_E_All_Data.zip'
df1 = download_zip_content(url1, headers={'User-Agent': 'Mozilla/5.0'})

df1.head()
save_to_sqlite(df1, 'temperature')

url2 = 'https://microdata.worldbank.org/index.php/catalog/4509/download/67079'
df2 = download_zip_content(url2, headers={'User-Agent': 'Mozilla/5.0'})
save_to_sqlite(df2, 'food_price_inflation')
