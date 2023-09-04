import pandas as pd
import json

# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)

# Leer el archivo JSON
json_file = "../data/data.json"
with open(json_file) as json_file:
    json_data = json.load(json_file)
    json_data_filtered = []

    for item in json_data:
        address = item['Address']
        distrito_parts = address['Distrito'].split(', ')
        nom_carrer = address['Calle']

        # Filtrar filas del CSV que contengan información del JSON
        filtered_rows = csv_df[csv_df.apply(lambda row: str(nom_carrer) in str(row['Nom_Carrer']) or str(distrito_parts[0]) in str(row['Nom_Districte']), axis=1)]


        # Si se encuentra alguna fila, agregar la información del JSON a la lista resultante
        if not filtered_rows.empty:
            for _, row in filtered_rows.iterrows():
                json_data_filtered.append({
                    'Codi_Districte': row['Codi_Districte'],
                    'Nom_Districte': row['Nom_Districte'],
                    'Nom_Carrer': row['Nom_Carrer'],
                    'title': item['title'],
                    'size': item['size'],
                    'price': item['price'],
                    'Vigilancia': item['Vigilancia'],
                    'Columnas': item['Columnas'],
                    'Tipo plaza': item['Tipo plaza'],
                    'Puerta': item['Puerta'],
                    'Planta': item['Planta'],
                    'Ciudad': address['Ciudad'],
                    'Personas': row['Personas'],
                    'Promedio_Euros': row['Promedio_Import_Euros'],
                    'Vehicles': row['Vehicles'],
                })

# Crear un DataFrame a partir de la lista de diccionarios
json_df = pd.DataFrame(json_data_filtered)

# Eliminar registros duplicados en el DataFrame resultante
json_df.drop_duplicates(inplace=True)

print(json_df)
# Guardar el resultado en un nuevo archivo JSON
json_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False)
