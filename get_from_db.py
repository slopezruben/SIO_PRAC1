import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt

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
print(neighborhoodTable)
#print(cityTable)

# Description By City
#listByCity = listingTable.merge(cityTable, how="inner", on='city_id')[['city','price_float','bathrooms','bedrooms']].groupby('city')
#listByCity.describe().to_csv("byCityDescription.csv")

#listingTypes = listingTable.merge(cityTable, how="inner", on='city_id')[['city','room_type']].groupby('room_type')
#print(listingTypes.describe())

listByReviewsByMonth = listingTable.merge(cityTable, how="inner", on='city_id')[['city', 'reviews_per_month']].groupby('city')
print(listByReviewsByMonth.describe())

listingTable = listingTable[['reviews_per_month','neighbourhood_id']]
print(listingTable.merge(neighborhoodTable, how="inner", on="neighbourhood_id")[['reviews_per_month','neighbourhood_cleansed']].groupby('neighbourhood_cleansed').describe())
