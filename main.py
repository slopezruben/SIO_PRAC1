import pandas as pd
from sqlalchemy import create_engine

diccionario = pd.read_csv('dataset/diccionario.csv')
diccionario = diccionario[['Field', 'Ignore','Table']].dropna()
df = pd.read_csv('dataset/mallorca.csv')

print(diccionario)

grupos_diccionario = diccionario.groupby(by=['Table'])
df_procesados = []

for field, grupo in grupos_diccionario:    
    campos = []
    for index, row in grupo.iterrows():
        campos.append(row['Field'])
    df_procesados.append(df.loc[:,campos])

for cositas in df_procesados:
    print(cositas)
