import pandas as pd
from sqlalchemy import create_engine, types
import pymysql
import os
#pymysql.install_as_MySQLdb()a
diccionarioHost={
        "host_id": types.Integer(),
        "host_name": types.Text(),
        "host_since": types.Text(),
        "host_location": types.Text(),
        "host_response_time": types.Text(),
        "host_response_rate": types.Text(),
        "host_acceptance_rate": types.Text(),
        }
diccionarioNeighborhood={
        "neighborhood": types.Text(),
        "neighborhood_cleansed": types.Text(),
        }
diccionarioListing={
        "id": types.Text(),
        "name": types.Text(),
        "latitude": types.Numeric(),
        "longitude": types.Numeric(),
        "property_type": types.Text(),
        "room_type": types.Text(),
        "accommodates": types.BigInteger(),
        "bathrooms": types.Numeric(),
        "bedrooms": types.Integer(),
        "beds": types.Integer(),
        "amenities": types.JSON(),
        "price": types.Text(),
        "minimum_nights_avg_ntm": types.Numeric(),
        "maximum_nights_avg_ntm": types.Numeric(),
        "availability_30": types.Integer(),
        "availability_60": types.Integer(),
        "availability_90": types.Integer(),
        "availability_365": types.Integer(),
        "number_of_reviews": types.Integer(),
        "review_scores_rating": types.Numeric(),
        "review_scores_accuracy": types.Numeric(),
        "review_scores_cleanliness": types.Numeric(),
        "review_scores_checkin": types.Numeric(),
        "review_scores_communication": types.Numeric(),
        "review_scores_location": types.Numeric(),
        "review_scores_value": types.Numeric(),
        "reviews_per_month": types.Numeric(),
        }

#Table Keys ['Host', 'Listing', 'Neighborhood']
def join_host_list(jlistingTable, jhostTable):
    joinedListingTable = jlistingTable.assign(host_id=jhostTable['host_id'] )
    return joinedListingTable

directorio = 'dataset'
ciudades = []
id_gen = 1

engine = create_engine('mysql+pymysql://xavi:ferrari_18@localhost/sio_db')
diccionario = pd.read_csv('diccionario.csv')
diccionario = diccionario[['Field', 'Ignore','Table']].dropna()

def get_tables(path):
    df = pd.read_csv(f'dataset/{path}.csv')

    grupos_diccionario = diccionario.groupby(by=['Table'])
    df_procesados = {}

    for table, grupo in grupos_diccionario:    
        campos = []
        for index, row in grupo.iterrows():
            campos.append(row['Field'])
        df_procesados[table]=df.loc[:,campos]


    subListingTable = df_procesados.get('Listing')
    subHostTable = df_procesados.get('Host')
    subNeighborhoodTable = df_procesados.get('Neighborhood')

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

cityTable = pd.DataFrame(ciudades, columns=['ciudad', 'id'])
print(cityTable)

listingTable.to_sql('Listing', con=engine, if_exists='replace',index=False, dtype=diccionarioListing)
hostTable.to_sql('Host', con=engine, if_exists='replace',index=False, dtype=diccionarioHost)
neighborhoodTable.to_sql('Neighborhood', con=engine, if_exists='replace',index=False, dtype=diccionarioNeighborhood)
cityTable.to_sql('City', con=engine, if_exists='replace',index=False)
