from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

user = 'root'
password = 'Welcome12345'
hostname = 'localhost'
port = '3306'
database = 'learning'
table = 'census_data'

engine_mysql = create_engine('mysql+mysqlconnector://' + user + ':' + password + '@' + hostname + ':' + port + '/' + database,poolclass=NullPool)

uri = "mongodb+srv://benjo:12345@cluster0.x4wlqah.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri, server_api=ServerApi('1'))
db = client["census"]  
collection = db["census_data"] 
cursor = collection.find()
data = list(cursor)
df_census = pd.DataFrame(data)
df_census = df_census.drop(['_id'], axis=1)
#print(df_census.columns)


    

df_census.to_sql(table,engine_mysql,index=False,if_exists='replace')
print('success')
