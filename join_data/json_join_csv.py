import pandas as pd
import json
from fuzzywuzzy import fuzz

# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)

with open("../data/data.json") as json_file:
    json_data = json.load(json_file)
    json_data_cleaned = []
    for item in json_data:
        address = item['Address']
        distrito_parts = address['Distrito'].split(', ')
        nom_carrer = address['Calle']
         # Buscar coincidencias difusas por el campo 'Nom_Carrer'
        matching_rows = csv_df[csv_df['Nom_Carrer'].apply(lambda x: fuzz.token_sort_ratio(x, nom_carrer)) > 80]
        
        # Si no hay coincidencias por 'Nom_Carrer', intentar por el campo 'Distrito'
        if matching_rows.empty:
            matching_rows = csv_df[csv_df['Nom_Districte'].apply(lambda x: fuzz.token_sort_ratio(x, distrito_parts[0])) > 80]
        if not matching_rows.empty:
            for _, matching_row in matching_rows.iterrows():
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
                    'Personas': matching_row['Personas'],
                    'Promedio_Euros': matching_row['Promedio_Import_Euros'],
                    'Vehicles': matching_row['Vehicles'],
                })
        else:
            # Si no hay una coincidencia en el campo 'Nom_Carrer', puedes agregar una fila con valores predeterminados
            # o simplemente omitirla. Aquí la omitimos.
            print(f"No se encontró una coincidencia para la calle: {nom_carrer}")
# Crear un DataFrame a partir de la lista de diccionarios
json_df = pd.DataFrame(json_data_cleaned)

# Eliminar registros duplicados en la columna "title"
json_df.drop_duplicates(subset='title', keep='first', inplace=True)
print(json_df)

# Guardar el resultado en un nuevo archivo JSON
json_df.to_json('../data/resultado_join.json', orient='records', lines=True, force_ascii=False)

