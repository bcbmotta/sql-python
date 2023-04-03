# Import libs
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import os
import pandas as pd

# Def function to rename tables
def table_name(csv):
    return csv.replace('olist_', '').replace('_dataset.csv', '')

# Env variables
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
DATABASE = os.getenv('DATABASE')

# Define connection string and create engine
string_conn = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine = create_engine(string_conn)

# List of CSVs to import
csv_files = os.listdir('archive')

# Create DATABASE
if not database_exists(engine.url):
    create_database(engine.url)

# Create TABLES
for csv in csv_files:
    if 'olist' in csv:
        df = pd.read_csv('archive/' + csv)
        df.to_sql(table_name(csv), engine, if_exists='replace', index=False)