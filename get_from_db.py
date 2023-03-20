import pandas as pd
from sqlalchemy import create_engine
import pymysql
#pymysql.install_as_MySQLdb()

engine = create_engine('mysql+pymysql://xavi:ferrari_18@localhost/sio_db')

with engine.connect() as conn:
    listingTable = pd.read_sql('Listing', con=conn)
    hostTable = pd.read_sql('Host', con=conn)
    neighborhoodTable = pd.read_sql('Neighborhood', con=conn)

print(listingTable)
print(hostTable)
print(neighborhoodTable)
