import pandas as pd
import json
# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)


with open("../data/data.json") as json_file:
    json_data = json.load(json_file)

# Crear una lista de diccionarios con los datos del JSON
json_data_cleaned = []
for item in json_data:
    address = item['Address']
    distrito_parts = address['Distrito'].split(', ')
    json_data_cleaned.append({
        'Codi_Districte': distrito_parts[1],  # Número del distrito desde JSON
        'Nom_Districte': distrito_parts[0],    # Nombre del distrito desde JSON
        'Nom_Carrer': address['Calle'],
        'title': item['title'],
        'size': item['size'],
        'price': item['price'],
        'Vigilancia': item['Vigilancia'],
        'Columnas': item['Columnas'],
        'Tipo plaza': item['Tipo plaza'],
        'Puerta': item['Puerta'],
        'Planta': item['Planta'],
        'Ciudad': address['Ciudad'],
        'Total_Numero_Places': csv_df['Total_Numero_Places'],
        'Personas': csv_df['Personas'],
        'Total_Import_Euros': csv_df['Total_Import_Euros'],
        'Vehicles': csv_df['Vehicles']
    })

# Crear un DataFrame a partir de la lista de diccionarios
json_df = pd.DataFrame(json_data_cleaned)

# Unir los DataFrames por Codi_Districte y Nom_Districte
merged_df = pd.merge(csv_df, json_df, left_on=['Codi_Districte', 'Nom_Districte'], right_on=['Codi_Districte', 'Nom_Districte'], how='inner')
print(merged_df)

# Guardar el resultado en un nuevo archivo CSV
merged_df.to_csv('../data/resultado_join.csv', index=False)
merged_df.to_json('../data/resultado_join.json', orient='records', lines=True)

