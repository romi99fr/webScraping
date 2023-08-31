import pymongo
import csv
import json

# Establir la connexió amb el servidor MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["db"]
json_collection = db["json_collection"]

# Crear col·leccions per CSV i JSON
csv_collection = db["csv_data"]
json_collection = db["json_data"]

# Desar dades des d'un fitxer CSV a la col·lecció csv_data
def save_csv_to_mongo(file_path):
    with open(file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            csv_collection.insert_one(row)

def save_json_to_mongo(json_file_path):
    with open(json_file_path, "r") as json_file:
        data_list = json.load(json_file)  # Cargar la lista de objetos JSON
        
        # Asegurarse de que data_list es una lista antes de continuar
        if isinstance(data_list, list):
            for data in data_list:
                json_collection.insert_one(data)  # Insertar cada objeto como un documento
                
            print("JSON data inserted successfully.")

if __name__ == "__main__":
    csv_file_path = "../csv_data/combined_df.csv"  # Canvia "ruta_del_fitxer.csv" amb la teva ruta
    json_file_path = "../data/data3.json"  # Canvia "ruta_del_fitxer.json" amb la teva ruta

    save_csv_to_mongo(csv_file_path)
    save_json_to_mongo(json_file_path)

    print("Dades desades a MongoDB amb èxit.")
