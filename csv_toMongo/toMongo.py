import pymongo
import csv
import json

# Establir la connexió amb el servidor MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]  # Canvia "mydatabase" pel nom de la teva base de dades

# Crear col·leccions per CSV i JSON
csv_collection = db["csv_data"]
json_collection = db["json_data"]

# Desar dades des d'un fitxer CSV a la col·lecció csv_data
def save_csv_to_mongo(file_path):
    with open(file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            csv_collection.insert_one(row)

# Desar dades des d'un fitxer JSON a la col·lecció json_data
def save_json_to_mongo(file_path):
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
        json_collection.insert_one(data)

if __name__ == "__main__":
    csv_file_path = "../csv_data/joined_result.csv"  # Canvia "ruta_del_fitxer.csv" amb la teva ruta
    json_file_path = "../data/data3.json"  # Canvia "ruta_del_fitxer.json" amb la teva ruta

    save_csv_to_mongo(csv_file_path)
    save_json_to_mongo(json_file_path)

    print("Dades desades a MongoDB amb èxit.")
