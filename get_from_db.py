import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt

user=input("insert dbase user: ")
password=input("insert password: ")

engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost/sio_db')

with engine.connect() as conn:
    listingTable = pd.read_sql('listing', con=conn)
    hostTable = pd.read_sql('host', con=conn)
    neighborhoodTable = pd.read_sql('neighborhood', con=conn)
    cityTable = pd.read_sql('city', con=conn)
    bathroomsTable = pd.read_sql('bathrooms',con=conn)
    hostVerificationTable = pd.read_sql('hostverification',con=conn)
    listingAmenitiesTable = pd.read_sql('listingamenities',con=conn)
    propertiesTable = pd.read_sql('properties',con=conn)
    roomsTable = pd.read_sql('rooms',con=conn)
    verificationsTable = pd.read_sql('verifications',con=conn)

print(cityTable)
mallorcaListing = listingTable[ listingTable['city_id'] == '2' ]
print(mallorcaListing[['id','city_id']])
