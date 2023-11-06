import pandas as pd
import zipfile
from io import BytesIO
import requests

headers = {'User-Agent': 'Mozilla/5.0'}

url = 'https://fenixservices.fao.org/faostat/static/bulkdownloads/Environment_Temperature_change_E_All_Data.zip'
r = requests.get(url, headers=headers)
buf1 = BytesIO(r.content)
with zipfile.ZipFile(buf1, "r") as f:
    for name in f.namelist():
        if name == 'Environment_Temperature_change_E_All_Data.csv':
            with f.open(name) as zd:
                df = pd.read_csv(zd, encoding='latin1')
            break


df.to_sql('temperature','sqlite:///../data/zylesto.sqlite', if_exists='replace', index=False)


df2 = pd.read_csv('https://microdata.worldbank.org/index.php/catalog/4509/download/65297', compression='zip')
df2.to_sql('food_price_inflation','sqlite:///../data/zylesto.sqlite', if_exists='replace', index=False)