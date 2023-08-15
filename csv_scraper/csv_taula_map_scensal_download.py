import os
import requests
import subprocess

# URL of the website containing the CSV file
url = "https://opendata-ajuntament.barcelona.cat/data/dataset/784cefda-9219-4b61-b5d5-68b5ac453070/resource/d43b851c-65a3-4e9f-9d71-33e91583b32f/download"


# Specify the local directory and file name to save the downloaded CSV
local_directory = "C:\\TFM\\webScraping\\csv_data"
local_file_name = "Taula_mapa_scensal.csv"
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


hadoop_bin = "../../hadoop-2.7.4/bin/hdfs"
put_command = [hadoop_bin, "dfs", "-put", "../csv_data/Taula_mapa_scensal.csv", "webScraping/Taula_mapa_scensal.csv"]

subprocess.run(put_command, check=True)
