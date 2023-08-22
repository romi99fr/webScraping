import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Lista de archivos CSV en HDFS
csv_files = [
    "webScraping/Adreces_per_secció_censal.csv",
    "webScraping/Infraestructures_Inventari_Reserves.csv",
    "webScraping/Taula_mapa_districte.csv",
    "webScraping/renda_neta_mitjana_per_persona.csv"
]

spark = SparkSession.builder.appName("HDFSFileRead").getOrCreate()

# Carpeta local donde se guardarán los archivos descargados
local_folder = "../csv_from_hdfs/"

# Crear la carpeta local si no existe
subprocess.run(["mkdir", "-p", local_folder])

# Descargar los archivos CSV desde HDFS
for csv_file in csv_files:
    subprocess.run(["../../hadoop-2.7.4/bin/hdfs", "dfs", "-get", csv_file, local_folder])

files = [
    "../csv_from_hdfs/Adreces_per_secció_censal.csv",
    "../csv_from_hdfs/Infraestructures_Inventari_Reserves.csv",
    "../csv_from_hdfs/Taula_mapa_districte.csv",
    "../csv_from_hdfs/renda_neta_mitjana_per_persona.csv"
]

# Definir las columnas a imprimir para cada archivo
columns_to_join = {
    "Adreces_per_secció_censal.csv": ["NOM_CARRER", "DISTRICTE", "SECC_CENS", "BARRI", "DPOSTAL"],
    "Infraestructures_Inventari_Reserves.csv": ["Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Numero_Places", "Desc_Tipus_Estacionament"],
    "Taula_mapa_districte.csv": ["Nom_Districte", "Sexe", "Nombre"],
    "renda_neta_mitjana_per_persona.csv": ["Any", "Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Seccio_Censal", "Import_Euros"]
}

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
        if file_name == "Taula_mapa_districte.csv":
            # Realizar operación de agregación en el DataFrame de Taula_mapa_districte.csv y pivoteo
            aggregated_df = df.groupBy("Nom_Districte", "Sexe").agg(F.sum("Nombre").alias("Total"))
            pivot_df = aggregated_df.groupBy("Nom_Districte").pivot("Sexe").agg(F.first("Total")).fillna(0)

            # Mostrar el DataFrame agregado y pivoteado
            print("DataFrame Tabla clean")
            pivot_df.show(truncate=False)

# Detener la sesión de Spark
spark.stop()