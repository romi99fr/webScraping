import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from functools import reduce

# Lista de archivos CSV en HDFS
csv_files = [
    "webScraping/Adreces_per_secció_censal.csv",
    "webScraping/Infraestructures_Inventari_Reserves.csv",
    "webScraping/Taula_mapa_districte.csv",
    "webScraping/renda_neta_mitjana_per_persona.csv",
    "webScraping/vehicles_districte.csv"
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
    "../csv_from_hdfs/Taula_mapa_districte.csv",
    "../csv_from_hdfs/renda_neta_mitjana_per_persona.csv",
    "../csv_from_hdfs/vehicles_districte.csv"
]

# Definir las columnas a imprimir para cada archivo
columns_to_join = {
    "Adreces_per_secció_censal.csv": ["NOM_CARRER", "DISTRICTE", "SECC_CENS", "BARRI", "DPOSTAL"],
    "Taula_mapa_districte.csv": ["Nom_Districte", "Sexe", "Nombre","Codi_Districte"],
    "renda_neta_mitjana_per_persona.csv": ["Any", "Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Seccio_Censal", "Import_Euros"],
    "vehicles_districte.csv": ["Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri", "Seccio_Censal", "Tipus_Servei","Total"],
}

modified_dfs = {}

for csv_file in files:
    file_name = csv_file.split("/")[-1]
    if file_name in columns_to_join:
        print(f"Fuente CSV {file_name}:")
        columns = columns_to_join[file_name]
        df = spark.read.csv(csv_file, header=True, inferSchema=True)
        df = df.select(*columns)
        
        if file_name == "vehicles_districte.csv":
            filtered_df = df.filter(df["Tipus_Servei"] == "Privat")
            aggregated_df = filtered_df.groupBy("Codi_Districte","Nom_Districte").agg(F.sum("Total").alias("Total")).fillna(0)
            pivot_df = aggregated_df.groupBy("Codi_Districte", "Nom_Districte").agg(F.first("Total").alias("Vehicles")).fillna(0)
            pivot_df.show(truncate=False)
            modified_dfs[file_name] = pivot_df

        if file_name == "Adreces_per_secció_censal.csv":
            filtered_df = df.select(col("NOM_CARRER"), col("DISTRICTE").alias("Codi_Districte"))
            filtered_df = filtered_df.distinct()
            filtered_df.show(truncate=False)
            modified_dfs[file_name] = filtered_df

        if file_name == "Taula_mapa_districte.csv":
            aggregated_df = df.groupBy("Codi_Districte", "Sexe").agg(F.sum("Nombre").alias("Total"))
            pivot_df = aggregated_df.groupBy("Codi_Districte").pivot("Sexe").agg(F.first("Total")).fillna(0)
             # Agregar una columna 'Personas' sumando los valores de 'Hombre' y 'Mujer'
            pivot_df = pivot_df.withColumn("Personas", pivot_df["Homes"] + pivot_df["Dones"])
            pivot_df = pivot_df.drop("Homes", "Dones")
            modified_dfs[file_name] = pivot_df
            # Mostrar el DataFrame agregado y pivoteado
            pivot_df.show(truncate=False)

        if file_name == "renda_neta_mitjana_per_persona.csv":
            # Realizar la agregación por distrito
            aggregated_df = df.groupBy("Codi_Districte").agg(F.sum("Import_Euros").alias("Total_Import_Euros"))

            # Mostrar el DataFrame agregado
            aggregated_df.show(truncate=False)
            modified_dfs[file_name] = aggregated_df

combined_df = reduce(lambda df1, df2: df1.join(df2, on="Codi_Districte", how="inner"), modified_dfs.values())
column_order = ['Codi_Districte', 'Nom_Districte', 'Nom_Carrer', 'Personas', 'Total_Import_Euros', 'Vehicles']
combined_df = combined_df[column_order]

combined_df.show(100)

combined_df_single_partition = combined_df.coalesce(1)
combined_pandas_df = combined_df_single_partition.toPandas()

# Save the Pandas DataFrame as a single CSV file
standalone_csv_path = "../csv_data/combined_df.csv"
combined_pandas_df.to_csv(standalone_csv_path, index=False)

# Upload the single CSV file to HDFS
hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
put_command = [hadoop_bin, "dfs", "-put", "-f", "../csv_data/combined_df.csv", "webScraping/combined_df.csv"]
subprocess.run(put_command, check=True)

# Detener la sesión de Spark
spark.stop() 