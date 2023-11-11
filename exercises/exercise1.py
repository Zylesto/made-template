import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, Float

df = pd.read_csv(
    'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv',
    engine='python',
    on_bad_lines='skip',
    sep=';'
)

engine = create_engine('sqlite:///airports.sqlite')
metadata = MetaData()

airports = Table('airports', metadata,
                 Column('column_1', Integer),
                 Column('column_2', String),
                 Column('column_3', String),
                 Column('column_4', String),
                 Column('column_5', String),
                 Column('column_6', String),
                 Column('column_7', Float),
                 Column('column_8', Float),
                 Column('column_9', Integer),
                 Column('column_10', Float),
                 Column('column_11', String),
                 Column('column_12', String),
                 Column('geo_punkt', Float),
                 )

metadata.create_all(engine)

df.to_sql('airports', con=engine, if_exists='replace', index=False)
