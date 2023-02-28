import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

#Table Keys ['Host', 'Listing', 'Neighborhood']
def join_host_list(listingTable, hostTable):
    joinedListingTable = listingTable.assign(host_id=hostTable['host_id'] )
    return joinedListingTable

engine = create_engine('mysql://ruben:ferrari_18@localhost/sio_db')
diccionario = pd.read_csv('dataset/diccionario.csv')
diccionario = diccionario[['Field', 'Ignore','Table']].dropna()
df = pd.read_csv('dataset/mallorca.csv')

print(diccionario)

grupos_diccionario = diccionario.groupby(by=['Table'])
df_procesados = {}

for table, grupo in grupos_diccionario:    
    campos = []
    for index, row in grupo.iterrows():
        campos.append(row['Field'])
    df_procesados[table]=df.loc[:,campos]


listingTable = df_procesados.get('Listing')
hostTable = df_procesados.get('Host')
neighboorhoodTable = df_procesados.get('Neighborhood')

df_procesados.update({'Listing': join_host_list(listingTable, hostTable)})

for name, table in df_procesados.items():
    table.to_sql(name, con=engine, if_exists='replace',index=False)

