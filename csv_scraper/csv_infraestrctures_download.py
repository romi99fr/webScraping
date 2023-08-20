import os
import requests
import subprocess

# URL of the website containing the CSV file
url = "https://opendata-ajuntament.barcelona.cat/data/dataset/14f13e0d-3feb-4688-ad63-e1ca9d1e479f/resource/93d8128d-417d-44da-87cb-df7420c40641/download"


# Specify the local directory and file name to save the downloaded CSV
local_directory = "../csv_data"
local_file_name = "Infraestructures_Inventari_Reserves.csv"
local_file_path = os.path.join(local_directory, local_file_name)

# Create the local directory if it doesn't exist
os.makedirs(local_directory, exist_ok=True)

# Send a GET request to the URL
response = requests.get(url)

if response.status_code == 200:
    # Write the content of the response to the local CSV file
    with open(local_file_path, "wb") as file:
        file.write(response.content)
    
    print("CSV file downloaded successfully.")
else:
    print(f"Failed to download CSV file. Status code: {response.status_code}")


# Definir los comandos por separado
hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
put_command = [hadoop_bin, "dfs", "-put", "../csv_data/Infraestructures_Inventari_Reserves.csv", "webScraping/Infraestructures_Inventari_Reserves.csv"]
subprocess.run(put_command, check=True)
