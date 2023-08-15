from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("HDFSFileRead").getOrCreate()

# Rutas de los archivos en HDFS
adreces_csv = "hdfs://master:27000/webScraping/Adreces_per_secció_censal.csv"
infra_csv = "hdfs://master:27000/webScraping/Infraestructures_Inventari_Reserves.csv"
taula_csv = "hdfs://master:27000/webScraping/Taula_mapa_scensal.csv"
renda_csv = "hdfs://master:27000/webScraping/renda_neta_mitjana_per_persona.csv"

# Leer los archivos CSV desde HDFS
df_adreces = spark.read.csv(adreces_csv, header=True, inferSchema=True)
df_infra = spark.read.csv(infra_csv, header=True, inferSchema=True)
df_taula = spark.read.csv(taula_csv, header=True, inferSchema=True)
df_renda = spark.read.csv(renda_csv, header=True, inferSchema=True)

# Mostrar los datos o realizar otras operaciones
df_adreces.show()
df_infra.show()
df_taula.show()
df_renda.show()

# Detener la sesión de Spark
spark.stop()
