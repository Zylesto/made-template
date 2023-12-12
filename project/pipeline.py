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

#############################################################################################################

#This URL has become unavailable during my project work, for the GitHub-Workflow i have disabled downloading and testing it

#url2 = 'https://microdata.worldbank.org/index.php/catalog/4509/download/67079'
#df2 = download_zip_content(url2, headers={'User-Agent': 'Mozilla/5.0'})
#save_to_sqlite(df2, 'food_price_inflation')

# get data from food_price_inflation table
#food_df = pd.read_sql_table('food_price_inflation', 'sqlite:///../data/zylesto.sqlite')

# extract month and year from the data column
#food_df['date'] = pd.to_datetime(food_df['date'])
#food_df['Months'] = food_df['date'].dt.month_name()
#food_df['Year'] = food_df['date'].dt.year

# keep only relevant columns
#food_df = food_df[['Inflation', 'country', 'Months', 'Year']]
#food_df = food_df.rename(columns={'country': 'Area'})

# remove the rows that contains missing values
#food_df = food_df.dropna()
#food_df

#############################################################################################################

url1 = 'https://fenixservices.fao.org/faostat/static/bulkdownloads/Environment_Temperature_change_E_All_Data.zip'
df1 = download_zip_content(url1, headers={'User-Agent': 'Mozilla/5.0'})
save_to_sqlite(df1, 'temperature')

temp_df = pd.read_sql_table('temperature', 'sqlite:///../data/zylesto.sqlite')

# keep only the rows that contains temperature change
temp_df = temp_df[temp_df['Element'] == 'Temperature change']
# drop irrelevant columns
temp_df = temp_df.drop(['Area Code', 'Area Code (M49)', 'Element Code', 'Months Code', 
                       'Unit', 'Element'], axis=1)

# loop through each years data and drop the rows where data is missing
yrs = range(1961, 2023)
for yr in yrs:
    temp_df = temp_df[temp_df[f'Y{yr}F'] == 'E']
    temp_df = temp_df.drop(f'Y{yr}F', axis=1)

# keep only those countries that are present in the food dataset
#temp_df = temp_df[temp_df['Area'].isin(food_df['country'].unique())]

# keep only monthly data
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
       'August', 'September', 'October', 'November', 'December']
temp_df = temp_df[temp_df['Months'].isin(months)]

# convert years columns into rows
value_vars = [f'Y{yr}' for yr in yrs]
temp_df = pd.melt(temp_df, id_vars=['Area', 'Months'], value_vars=value_vars,
                 var_name='Year', value_name='Change')

# remove Y from each year and convert datatype to int
temp_df['Year'] = temp_df['Year'].str[1:].astype(int)
