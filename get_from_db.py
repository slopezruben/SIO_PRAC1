import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
#pymysql.install_as_MySQLdb()
user=input("insert dbase user: ")
password=input("insert password: ")

engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost/sio_db')

with engine.connect() as conn:
    listingTable = pd.read_sql('Listing', con=conn)
    hostTable = pd.read_sql('Host', con=conn)
    neighborhoodTable = pd.read_sql('Neighborhood', con=conn)
    cityTable = pd.read_sql('City', con=conn)

#print(listingTable)
#print(hostTable)
#print(neighborhoodTable)
print(cityTable)

listByCity = listingTable.merge(cityTable, how="inner", on='city_id')[['city','price_float']].groupby('city')
print(listByCity.describe().to_csv("description.csv"))
