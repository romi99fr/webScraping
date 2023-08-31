import pandas as pd
import json
# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)


# Cargar el JSON y crear un DataFrame con las columnas necesarias
with open('../data/data.json') as json_file:
    json_data = json.load(json_file)
    json_data_cleaned = []
    for item in json_data:
        address = item['Address']
        distrito_parts = address['Distrito'].split(', ')
        json_data_cleaned.append({
            'Codi_Districte': distrito_parts[1],  # Número del distrito desde JSON
            'Nom_Districte': distrito_parts[0],    # Nombre del distrito desde JSON
            'Nom_Carrer': address['Calle']
        })
    json_df = pd.DataFrame(json_data_cleaned)

csv_df['Codi_Districte'] = csv_df['Codi_Districte'].astype(str)
json_df['Codi_Districte'] = json_df['Codi_Districte'].astype(str)

# Unir los DataFrames por Codi_Districte y Nom_Districte
merged_df = pd.merge(csv_df, json_df, left_on=['Codi_Districte', 'Nom_Districte'], right_on=['Codi_Districte', 'Nom_Districte'], how='inner')
merged_df = merged_df.drop(columns=['NOM_CARRER'])
column_order = ['Codi_Districte', 'Nom_Districte', 'Nom_Carrer', 'Total_Numero_Places', 'Personas', 'Total_Import_Euros', 'Vehicles']
merged_df = merged_df[column_order]
print(merged_df)

# Guardar el resultado en un nuevo archivo CSV
merged_df.to_csv('../data/resultado_join.csv', index=False)
merged_df.to_json('../data/resultado_join.json', orient='records', lines=True)

