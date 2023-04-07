import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
#pymysql.install_as_MySQLdb()

engine = create_engine('mysql+pymysql://xavi:ferrari_18@localhost/sio_db')

with engine.connect() as conn:
    listingTable = pd.read_sql('Listing', con=conn)
    hostTable = pd.read_sql('Host', con=conn)
    neighborhoodTable = pd.read_sql('Neighborhood', con=conn)

#print(listingTable)
#print(hostTable)
#print(neighborhoodTable)

#print(hostTable[["host_since","host_acceptance_rate"]])
#hostTable[["host_since","host_acceptance_rate"]].plot.scatter(x='host_since',y='host_acceptance_rate',c='DarkBLue')

subHost = hostTable[["host_since","host_acceptance_rate"]].isna() #tremenda fumada bro
subHost.plot()
plt.show()
