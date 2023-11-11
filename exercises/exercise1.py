import pandas as pd
from sqlalchemy import INTEGER, String,Float, DECIMAL

data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"
df = pd.read_csv(data_url, sep=';')

df.head()

columnTypes = {'column_1': INTEGER, 'column_2': String, 'column_3': String, 'column_4': String, 'column_5': String,
               'column_6': String, 'column_7': Float,'column_8': Float, 'column_9': INTEGER, 'column_10': Float,
               'column_11': String,'column_12': String, 'geo_punkt': Float}


df.to_sql('airports','airports.sqlite', if_exists='replace', index=False)