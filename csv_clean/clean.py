import subprocess
import pandas as pd

# Lista de archivos CSV en HDFS
csv_files = [
    "webScraping/Adreces_per_secció_censal.csv",
    "webScraping/Infraestructures_Inventari_Reserves.csv",
    "webScraping/Taula_mapa_scensal.csv",
    "webScraping/renda_neta_mitjana_per_persona.csv"
]

# Carpeta local donde se guardarán los archivos descargados
local_folder = "hdfs_downloads/"

# Crear la carpeta local si no existe
subprocess.run(["mkdir", "-p", local_folder])

# Descargar los archivos CSV desde HDFS
for csv_file in csv_files:
    subprocess.run(["hadoop-2.7.4/bin/hdfs", "dfs", "-get", csv_file, local_folder])

# Mostrar el contenido de los archivos CSV descargados
for csv_file in csv_files:
    local_path = local_folder + csv_file.split("/")[-1]  # Obtener solo el nombre del archivo
    print(f"Contenido de {local_path}:")
    
    df = pd.read_csv(local_path)
    print(df)
    print("-" * 50)

print("Fin del proceso.")
