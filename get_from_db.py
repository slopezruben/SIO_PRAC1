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

def get_frequency_plot(df1, df2, join_on, column_name, column_list, file_name, top=10):
    if not column_list:
        df = df1
    else:
        df = join_dataframes(df1, df2, join_on, column_list)

    df_freq = pd.DataFrame(df[column_name].value_counts())
    df_freq = df_freq.sort_values(by=column_name, ascending=False).head(n=top)

    df_freq.plot(use_index=True, y=column_name, kind='bar')
    plt.xlabel(column_name)
    plt.ylabel('Freqüencia')

    plt.savefig(file_name, bbox_inches='tight')

def join_dataframes(df1, df2, join_on, column_list):
        return df1.merge(df2, how='inner', on=join_on)[column_list]

with engine.connect() as conn:
    listingTable = pd.read_sql('listing', con=conn)
    hostTable = pd.read_sql('host', con=conn)
    neighborhoodTable = pd.read_sql('neighborhood', con=conn)
    cityTable = pd.read_sql('city', con=conn)
    bathroomsTable = pd.read_sql('bathrooms',con=conn)
    hostVerificationTable = pd.read_sql('hostverification',con=conn)
    listingAmenitiesTable = pd.read_sql('listingamenities',con=conn)
    amenitiesTable = pd.read_sql('amenities', con=conn)
    propertiesTable = pd.read_sql('properties',con=conn)
    roomsTable = pd.read_sql('rooms',con=conn)
    verificationsTable = pd.read_sql('verifications',con=conn)

# City Listings
mallorcaListing = get_city_table('mallorca')
get_scatter_plot(mallorcaListing, bathroomsTable, 'bathrooms_text_id', ['bathrooms','price_float'], 'menorquitaMoney1')
get_frequency_plot(mallorcaListing, bathroomsTable, 'bathrooms_text_id', 'bathrooms_text', ['bathrooms_text'], 'bathroomFreq')

get_frequency_plot(listingAmenitiesTable, amenitiesTable, 'amenities_id', 'amenities', ['amenities'], 'commonammenities',top=50)