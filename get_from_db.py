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
    bathroomsTable = pd.read_sql('Bathrooms',con=conn)
    hostVerificationTable = pd.read_sql('HostVerification',con=conn)
    listingAmenitiesTable = pd.read_sql('ListingAmenities',con=conn)
    propertiesTable = pd.read_sql('Properties',con=conn)
    roomsTable = pd.read_sql('Rooms',con=conn)
    verificationsTable = pd.read_sql('Verifications',con=conn)

print(cityTable)
mallorcaListing = listingTable[ listingTable['city_id'] == '2' ]
print(mallorcaListing[['id','city_id']])
