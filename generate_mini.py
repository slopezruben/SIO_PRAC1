import os
import pandas as pd
directorio="dataset"

for archivo in os.listdir(directorio):
        if os.path.isfile(os.path.join(directorio,archivo)):
            ciudad, extension = os.path.splitext(archivo)
            city_df = pd.read_csv(f"dataset/{archivo}")
            
            new_df = city_df.iloc[0:50]
            new_df.to_csv(f"dataset_mini/{ciudad}.csv")

