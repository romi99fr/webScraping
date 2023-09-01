import pymongo
import csv
import json

# Establir la connexió amb el servidor MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["db"]
json_collection = db["json_collection"]

def save_json_to_mongo(json_file_path):
    # Abre el archivo JSON y procesa línea por línea
    with open(json_file_path, "r") as json_file:
        for line in json_file:
            try:
                # Carga cada línea como un objeto JSON
                json_data = json.loads(line)
<<<<<<< HEAD
            
                # Inserta el objeto JSON en la colección
                json_collection.insert_one(json_data)
            
=======

                # Inserta el objeto JSON en la colección
                json_collection.insert_one(json_data)

>>>>>>> 33b74a3385986e53df6d41e905df61c32b049292
            except json.JSONDecodeError:
                print("Error de decodificación JSON en la línea:", line)
    print(json_collection)
    print("Datos JSON insertados en MongoDB exitosamente.")


if __name__ == "__main__":
    json_file_path = "../data/resultado_join.json"  # Canvia "ruta_del_fitxer.json" amb la teva ruta

    save_json_to_mongo(json_file_path)

    print("Dades desades a MongoDB amb èxit.")

