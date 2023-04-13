import math
import pandas as pd
from config import *
from sqlalchemy import create_engine 
import pymysql
import os

# Diccionario de los DataFrames
# Sirve para que el motor pueda identificar que tipo es cada variable del DataFrame

# Table Keys ['Host', 'Listing', 'Neighborhood']
# Funcion para enlazar las ids de los host con sus respectivos alquileres
def join_host_list(jlistingTable, jhostTable):
    joinedListingTable = jlistingTable.assign(host_id=jhostTable['host_id'] )
    return joinedListingTable

directorio = 'dataset'
ciudades = []
id_gen = 1

# Pasamos el nombre y contraseña de la base de datos
user=input("insert db username: ")
password=input("insert password: ")

# Creación del motor para conectar con la base de datos
engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost/sio_db')
# Lectura del diccionario
# Hace falta actualizar el CSV cada vez que añadamos una variable procesada
diccionario = pd.read_csv('diccionario.csv')
diccionario = diccionario[['Field', 'Ignore','Table']].dropna()


def parseToFloatPrice(price):
    result = float(price.strip('$').replace(',',''))
    return result

def parseTextToInt(text):
    u=str(text).split(" ")[0]
    try:
        return(int(math.floor(float(u))))
    except:
        return 1
def parseCurrency(number, rate):
    try:
        return number*rate
    except:
        return 0

def get_tables(path):
    #Lectura del dataframe por ciudades
    df = pd.read_csv(f'dataset/{path}.csv')
    # AÑADIR AQUI EL PROCESAMIENTO DE LAS VARIABLES
    # REALIZAR SOBRE EL DF ORIGINAL PARA EL PROCESAMIENTO
    

    df["price_float"] = df["price"].apply(parseToFloatPrice)
    rate=currencyRates.get(path)
    print(rate)
    df["price_float"] = df["price_float"].apply(parseCurrency, rate=rate)
    df['bathrooms'] = df['bathrooms_text'].apply(parseTextToInt)  
    
    # FIN DEL PROCESAMIENTO
    
    grupos_diccionario = diccionario.groupby(by=['Table'])
    df_procesados = {}
    

    for table, grupo in grupos_diccionario:    
        campos = []
        for row in grupo.iterrows():
            campos.append(row[1]['Field'])
        df_procesados[table]=df.loc[:,campos]

    subListingTable = df_procesados.get('Listing')
    subHostTable = df_procesados.get('Host')
    subNeighborhoodTable = df_procesados.get('Neighborhood')
    subNeighborhoodTable = subNeighborhoodTable[['neighbourhood_cleansed']].drop_duplicates() 
    
    subListingTable['city_id'] = id_gen-1
    df_procesados.update({'Listing': join_host_list(subListingTable, subHostTable)})

    return subListingTable, subHostTable, subNeighborhoodTable

for archivo in os.listdir(directorio):
    if os.path.isfile(os.path.join(directorio,archivo)):
        ciudad, extension = os.path.splitext(archivo)
        ciudades.append((ciudad,id_gen))
        id_gen += 1
    subListingTable,subHostTable,subNeighborhoodTable = get_tables(ciudad)
    if id_gen == 2 :
        listingTable = subListingTable
        hostTable = subHostTable
        neighborhoodTable = subNeighborhoodTable
    else:
        listingTable = pd.concat([listingTable, subListingTable])
        hostTable = pd.concat([hostTable, subHostTable])
        neighborhoodTable = pd.concat([neighborhoodTable, subNeighborhoodTable])
cityTable = pd.DataFrame(ciudades, columns=['city', 'city_id'])

# Añadir nuevos indices unicos a los barrios
neighborhoodTable.reset_index(inplace=True)
neighborhoodTable['neighbourhood_id'] = neighborhoodTable.index + 101
neighborhoodTable= neighborhoodTable.drop(columns='index')

# 
listingTable = listingTable.merge(neighborhoodTable, on='neighbourhood_cleansed', how='left')
listingTable.drop('neighbourhood_cleansed', axis=1, inplace=True)

listingTable.to_csv("listing_with_neigh_id.csv")

# Cargar a la BD
listingTable.to_sql('Listing', con=engine, if_exists='replace',index=False, dtype=diccionarioListing)
hostTable.to_sql('Host', con=engine, if_exists='replace',index=False, dtype=diccionarioHost)
neighborhoodTable.to_sql('Neighborhood', con=engine, if_exists='replace',index=False, dtype=diccionarioNeighborhood)
cityTable.to_sql('City', con=engine, if_exists='replace',index=False)

