import pymongo
import csv
import json

# Establir la connexió amb el servidor MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["db"]
json_collection = db["json_collection"]

def save_json_to_mongo(json_file_path):
    # Lista para almacenar los objetos JSON
    json_list = []

    # Abre el archivo JSON y procesa línea por línea
    with open(json_file_path, "r") as json_file:
        for line in json_file:
            try:
                # Carga cada línea como un objeto JSON y agrégala a la lista
                json_data = json.loads(line)
                json_list.append(json_data)

            except json.JSONDecodeError:
                print("Error de decodificación JSON en la línea:", line)

    # Inserta la lista de objetos JSON en la colección
    if json_list:
        json_collection.insert_many(json_list)



if __name__ == "__main__":
    json_file_path = "../data/resultado_join.json"

    save_json_to_mongo(json_file_path)

    print("Dades desades a MongoDB amb èxit.")

