import pandas as pd
import json
# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)

with open("../data/data.json") as json_file:
    json_data = json.load(json_file)
    json_data_cleaned = []
    for item in json_data:
        address = item['Address']
        distrito_parts = address['Distrito'].split(', ')
        nom_carrer = address['Calle']  # Obtener el nombre de la calle desde el JSON
        matching_row = csv_df[csv_df['Nom_Carrer'] == nom_carrer]
        if not matching_row.empty:
            json_data_cleaned.append({
                'Codi_Districte': distrito_parts[1],  # Número del distrito desde JSON
                'Nom_Districte': distrito_parts[0],   # Nombre del distrito desde JSON
                'Nom_Carrer': nom_carrer,            # Nombre de la calle desde JSON
                'title': item['title'],              # Otros datos del JSON
                'size': item['size'],
                'price': item['price'],
                'Vigilancia': item['Vigilancia'],
                'Columnas': item['Columnas'],
                'Tipo plaza': item['Tipo plaza'],
                'Puerta': item['Puerta'],
                'Planta': item['Planta'],
                'Ciudad': address['Ciudad'],
                # Agregando columnas del CSV
                'Personas': matching_row['Personas'].values[0],
                'Promedio_Euros': matching_row['Promedio_Import_Euros'].values[0],
                'Vehicles': matching_row['Vehicles'].values[0],
            })

# Crear un DataFrame a partir de la lista de diccionarios
json_df = pd.DataFrame(json_data_cleaned)

print(json_df)

# Guardar el resultado en un nuevo archivo JSON
json_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False)

# Guardar el resultado en un nuevo archivo CSV
json_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False)

