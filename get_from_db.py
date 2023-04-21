import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt

#user=input("insert dbase user: ")
#password=input("insert password: ")
user='ruben'
password='ferrari_18'
engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost/sio_db')

def get_city_table(city_name):
    index = (cityTable.loc[cityTable['city'] == city_name, 'city_id'].tolist()[0])
    return listingTable[listingTable['city_id'] == str(index)]
##########################
#   df1, df2 = dataframes, si necesitas información de solo el primero pasa el mismo dos veces
#   join_on = string, nombre de la columna en la que hacer el join, pasarle '' en caso de que solo evalues un dataframe
#   column_list = lista de longitud 2, le pasas los nombres de las dos columnas a analizar
#   file_name = string, nombre de la imagen
#########################
def get_scatter_plot(df1, df2, join_on, column_list, file_name):
    if join_on == '':
        print("no join on")
        correlated_dataframe = df1
    else:
        correlated_dataframe = join_dataframes(df1, df2, join_on, column_list)

    coef = np.polyfit(correlated_dataframe[column_list[0]], correlated_dataframe[column_list[1]], 1)
    poly1d_fn = np.poly1d(coef)

    correlated_dataframe.plot(x=column_list[0], y=column_list[1], kind='scatter')
    plt.plot(correlated_dataframe[column_list[0]], poly1d_fn(correlated_dataframe[column_list[0]]), c='r')
    plt.savefig(file_name)

##########################
#   df1, df2 = dataframes, si necesitas información de solo el primero pasa el mismo dos veces
#   join_on = string, nombre de la columna en la que hacer el join, pasarle '' en caso de que solo evalues un dataframe
#   column_name = string, nombre de la columna a analizar 
#   file_name = string, nombre de la imagen
#########################
def get_frequency_plot(df1, df2, join_on, column_name, file_name, top=10):
    if join_on == '':
        df = df1
    else:
        df = join_dataframes(df1, df2, join_on, [column_name])

    df_freq = pd.DataFrame(df[column_name].value_counts())
    df_freq = df_freq.sort_values(by=column_name, ascending=False).head(n=top)

    df_freq.plot(use_index=True, y=column_name, kind='bar')
    plt.xlabel(column_name)
    plt.ylabel('Freqüencia')

    plt.savefig(file_name, bbox_inches='tight')

def join_dataframes(df1, df2, join_on, column_list):
        return df1.merge(df2, how='inner', on=join_on)[column_list]

with engine.connect() as conn:
    listingTable = pd.read_sql('listing', con=conn).fillna(0)
    hostTable = pd.read_sql('host', con=conn).fillna(0)
    neighborhoodTable = pd.read_sql('neighborhood', con=conn).fillna(0)
    cityTable = pd.read_sql('city', con=conn).fillna(0)
    bathroomsTable = pd.read_sql('bathrooms',con=conn).fillna(0)
    hostVerificationTable = pd.read_sql('hostverification',con=conn).fillna(0)
    listingAmenitiesTable = pd.read_sql('listingamenities',con=conn).fillna(0)
    amenitiesTable = pd.read_sql('amenities', con=conn).fillna(0)
    propertiesTable = pd.read_sql('properties',con=conn).fillna(0)
    roomsTable = pd.read_sql('rooms',con=conn).fillna(0)
    verificationsTable = pd.read_sql('verifications',con=conn).fillna(0)

# City Listings
mallorcaListing = get_city_table('mallorca')
get_scatter_plot(mallorcaListing, bathroomsTable, 'bathrooms_text_id', ['bathrooms','price_float'], 'menorquitaMoney1')
get_frequency_plot(mallorcaListing, bathroomsTable, 'bathrooms_text_id', 'bathrooms_text', 'bathroomFreq')

get_frequency_plot(listingAmenitiesTable, amenitiesTable, 'amenities_id', 'amenities', 'commonammenities',top=50)