import os
import requests
import subprocess

# URL of the website containing the CSV file
url = "https://opendata-ajuntament.barcelona.cat/data/dataset/620d9bd8-54e6-4d7a-88b6-4a54c40c2dc6/resource/96b2b713-7fe0-4e79-a842-0a9b2e7bffe3/download"


# Specify the local directory and file name to save the downloaded CSV
local_directory = "C:\\TFM\\webScraping\\csv_data"
local_file_name = "Adreces per secció censal.csv"
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
            "../csv_data/'Adreces per secció censal.csv",
            "webScraping/Adreces_per_secció_censal.csv"
        ],
        check=True
    )