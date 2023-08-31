import pandas as pd

# Leer el archivo CSV en un DataFrame
csv_file = "../csv_data/combined_df.csv"
csv_df = pd.read_csv(csv_file)

# Leer el archivo JSON en un DataFrame
json_file = "../data/data.json"
json_df = pd.read_json(json_file)

# Unir los DataFrames por filas (concatenación vertical)
combined_df = pd.concat([csv_df, json_df], ignore_index=True)

# Exportar el DataFrame resultante a un nuevo archivo CSV
combined_csv_file = "../data/final_data.csv"
combined_df.to_csv(combined_csv_file, index=False)

# O exportar el DataFrame resultante a un nuevo archivo JSON
combined_json_file = "../data/final_data.json"
combined_df.to_json(combined_json_file, orient="records", lines=True)
