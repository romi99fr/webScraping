import os
import requests
import subprocess

# URL of the website containing the CSV file
url = "https://opendata-ajuntament.barcelona.cat/data/dataset/b7ba32eb-806e-4c9c-b0b1-9bab387fe501/resource/540e48d8-c432-43df-b3ba-a0cf009b90ef/download"


# Specify the local directory and file name to save the downloaded CSV
local_directory = "../csv_data/"
local_file_name = "Densitat.csv"
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


subprocess.run(
        [
            "../../hadoop-2.7.4/bin/hdfs",
            "dfs",
            "-put",
            "../csv_data/'Densitat.csv",
            "webScraping/Densitat.csv"
        ],
        check=True
    )