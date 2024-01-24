import urllib.request
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from sqlalchemy import create_engine

gtfs_url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zipfile_name = 'GTFS.zip'
urllib.request.urlretrieve(gtfs_url, zipfile_name)

with ZipFile(zipfile_name,'r') as zip_ref:
    stops_data = pd.read_csv(BytesIO(zip_ref.read('stops.txt')))

stops_data = stops_data[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
stops_data = stops_data[stops_data['zone_id'] == 2001]

stops_data = stops_data[(stops_data['stop_lat'].between(-90, 90)) & (stops_data['stop_lon'].between(-90, 90))]

engine = create_engine('sqlite:///gtfs.sqlite')

stops_data.to_sql('stops', engine, if_exists='replace', index=False)
