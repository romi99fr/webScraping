import subprocess
from pyspark.sql import SparkSession

# Lista de archivos CSV en HDFS
csv_files = [
    "webScraping/Adreces_per_secció_censal.csv",
    "webScraping/Infraestructures_Inventari_Reserves.csv",
    "webScraping/Taula_mapa_scensal.csv",
    "webScraping/renda_neta_mitjana_per_persona.csv"
]

spark = SparkSession.builder.appName("HDFSFileRead").getOrCreate()

# Carpeta local donde se guardarán los archivos descargados
local_folder = "../hdfs_from_hdfs/"

# Crear la carpeta local si no existe
subprocess.run(["mkdir", "-p", local_folder])

# Descargar los archivos CSV desde HDFS
for csv_file in csv_files:
    subprocess.run(["../../hadoop-2.7.4/bin/hdfs", "dfs", "-get", csv_file, local_folder])

files = [
    "../csv_from_hdfs/Adreces_per_secció_censal.csv",
    "../csv_from_hdfs/Infraestructures_Inventari_Reserves.csv",
    "../csv_from_hdfs/Taula_mapa_scensal.csv",
    "../csv_from_hdfs/renda_neta_mitjana_per_persona.csv"
]

# Definir las columnas a imprimir para cada archivo
columns_to_join = {
    "Adreces_per_secció_censal.csv": ["NOM_CARRER", "DISTRICTE", "SECC_CENS", "BARRI", "DPOSTAL"],
    "Infraestructures_Inventari_Reserves.csv": ["Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Numero_Places", "Desc_Tipus_Estacionament"],
    "Taula_mapa_scensal.csv": ["SECCIO_CENSAL", "HOMES", "DONES"],
    "renda_neta_mitjana_per_persona.csv": ["Any", "Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Seccio_Censal", "Import_Euros"]
}

# Definir las columnas de join en orden de prioridad
join_columns = ["Nom_Districte", "Districte"]

combined_df = None

# Procesar y mostrar las columnas para cada archivo CSV
for csv_file in files:
    file_name = csv_file.split("/")[-1]
    if file_name in columns_to_join:
        print(f"Fuente CSV {file_name}:")
        print(", ".join(columns_to_join[file_name]))
        columns = columns_to_join[file_name]
        df = spark.read.csv(csv_file, header=True, inferSchema=True)
        df = df.select(*columns)
        df.show(truncate=False)

        if combined_df is None:
            combined_df = df
        else:
            join_column = None
            for column in join_columns:
                if column in combined_df.columns:
                    join_column = column
                    break
            if join_column:
                combined_df = combined_df.join(df, on=join_column, how="full")

# Mostrar el DataFrame combinado
print("DataFrame combinado:")
combined_df.show(truncate=False)


# Guardar el DataFrame combinado como archivo CSV
output_path = "combined.csv"
combined_df.write.csv(output_path, header=True, mode="overwrite")

# Detener la sesión de Spark
spark.stop()