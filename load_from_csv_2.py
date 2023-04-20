import math, os, ast, pymysql
import pandas as pd
import numpy as np

from config import *
from sqlalchemy import create_engine 

directorio = 'dataset'
ciudades = []
id_gen = 1

# Pasamos el nombre y contraseña de la base de datos
user=input("insert db username: ")
password=input("insert password: ")

# Creación del motor para conectar con la base de datos
engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost/sio_db?charset=utf8mb4')

def parseToFloatPrice(price):
    result = float(price.strip('$').replace(',',''))
    return result

def parseTextToFloat(text):
    u=str(text).split(" ")[0]
    try:
        return(float(u))
    except:
        return 0.5

def parseCurrency(number, rate):
    try:
        return number*rate
    except:
        return 0

def compute_column(df, new_column, computable_column, function):
    df[new_column] = df[computable_column].apply(function)
    return df    

def generate_dataframe(dataframe, columns_dict, key_column):
    subList=[]
    for string in list(columns_dict.keys()):
        if string not in diccionarioId:
            subList.append(string)
    sub_dataframe = dataframe.loc[:,subList]
    sub_dataframe = sub_dataframe.drop_duplicates()
    
    id_column_name = key_column+'_id'

    sub_dataframe[id_column_name] = np.arange(len(sub_dataframe))
 
    dataframe=dataframe.merge(sub_dataframe[[key_column, id_column_name]], on=key_column)
    columns_to_drop = [col for col in subList]
    dataframe.drop(columns_to_drop, axis=1, inplace=True)
    
    # Cambiamos el nombre de la columna restante por <nombre de la llave>_id
    dataframe.rename(columns={key_column: id_column_name}, inplace=True)
    return dataframe, sub_dataframe

def generate_mn_table(df, column_name, id_column_name):
    # Convertimos la columna de string a lista
    df[column_name] = df[column_name].apply(lambda x: ast.literal_eval(x))

    # Generamos un dataframe con los elementos únicos de la lista y un identificador único
    elements = []
    for row in df[column_name]:
        if row == None : continue 
        elements += row
    elements = list(set(elements))
    elements_df = pd.DataFrame({column_name: elements, 'id': range(len(elements))})
    # Generamos un dataframe que relaciona los identificadores de cada fila del dataframe original con los identificadores
    # de los elementos que le pertenecen
    ids = []
    element_ids = []
    for i, row in df.iterrows():
        row_id = row[id_column_name]
        if row[column_name] == None: continue
        for element in row[column_name]:
            element_id = elements_df[elements_df[column_name] == element]['id'].values[0]
            ids.append(row_id)
            element_ids.append(element_id)
    ids_df = pd.DataFrame({'id': ids, 'element_id': element_ids})
    
    df=df.drop(columns=column_name)
    return df, elements_df, ids_df

# Crear un sub dataframe de la ciudad, añadirle al sub data frame la id de la ciudad, 
# juntarlo con el df grande

df = pd.DataFrame()

for archivo in os.listdir(directorio):
    try:
        if os.path.isfile(os.path.join(directorio,archivo)):
            ciudad, extension = os.path.splitext(archivo)
            ciudades.append((ciudad,id_gen))
            id_gen += 1
            city_df = pd.read_csv(f"dataset/{archivo}")
            print(ciudad)
            city_df['city_id'] = id_gen-1
            rate=currencyRates.get(ciudad)
            city_df = compute_column(city_df,new_column="price_float",computable_column='price',function=parseToFloatPrice)
            city_df['price_float'] = city_df['price_float'].apply(parseCurrency, rate=rate) 
            city_df['bathrooms'] = city_df['bathrooms_text'].apply(parseTextToFloat)
            print(df)
            if df.empty:
                df = city_df.copy()
            else:  
                print('df concat')
                df = pd.concat([df, city_df])
                print(df)
    except:
        print("no s'ha pogut generar la llista de ciutats, cancelant la operacio")
        exit(1)
print("Creando dataframe de la ciudad")
cityTable = pd.DataFrame(ciudades, columns=['city', 'city_id'])

print("Creando el dataframe de las habitaciones")
df, roomTable = generate_dataframe(df, {'room_type': types.Text()}, 'room_type')
print("Creando el dataframe de las vecinidades")
df, neighborhoodTable = generate_dataframe(df, diccionarioNeighborhood, 'neighbourhood_cleansed')
print("Creando dataframe de los lavabos")
df, bathroomTable = generate_dataframe(df, diccionarioBathroom, 'bathrooms_text')
print("Creando dataframe de las propiedades")
df, propertyTable = generate_dataframe(df, diccionarioProperty, 'property_type')


print("Creando el dataframe de los Hosts")
auxKeys = diccionarioHost.copy()
auxKeys.update({'host_verifications': types.Text()})
df['host_id_to_listing'] = df.loc[:,'host_id']
df, hostTable = generate_dataframe(df, auxKeys, 'host_id')
hostTable.drop(columns='host_id_id', inplace=True)
print("Creando m:n hosts verificaciones")
hostTable, verificationTable, mnHostVeriTable = generate_mn_table(hostTable, 'host_verifications', 'host_id')
df.rename(columns={'host_id_to_listing': 'host_id'},inplace=True)


print("Crea m:n listing amenities")
auxKeys = list(diccionarioListing.keys())
auxKeys.append('amenities')

listingTable = df.loc[:, auxKeys]
listingTable, amenitiesTable, mnListAmenTable = generate_mn_table(listingTable, 'amenities', 'id')



# Cargar a la BD
print("----CARGANDO EN LA BASE DE DATOS...---")
print("LISTINGS")
listingTable.to_sql('Listing'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioListing)
print("HOSTS")
hostTable.to_sql('Host'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioHost)
print("NEIGHBORHOODS")
neighborhoodTable.to_sql('Neighborhood'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioNeighborhood)
print("CITIES")
cityTable.to_sql('City'.lower(), con=engine, if_exists='replace',index=False)
print("ROOMS")
roomTable.to_sql('Rooms'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioRoom)
print("BATHSROOMS")
bathroomTable.to_sql('Bathrooms'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioBathroom)
print("PROPERTY TYPES")
propertyTable.to_sql('Properties'.lower(), con=engine, if_exists='replace',index=False, dtype=diccionarioProperty)
print("VERIFICATIONS")
verificationTable.to_sql('Verifications'.lower(), con=engine, if_exists='replace',index=False)
print("AMENITIES")
amenitiesTable.to_sql('Amenities'.lower(), con=engine, if_exists='replace',index=False)
print("M:N LISTING AMENITIES")
mnListAmenTable.to_sql('ListingAmenities'.lower(), con=engine, if_exists='replace',index=False)
print("M:N HOST VERIFICATIONS")
mnHostVeriTable.to_sql('HostVerification'.lower(), con=engine, if_exists='replace',index=False)

print(df.columns)
print(df.head())
