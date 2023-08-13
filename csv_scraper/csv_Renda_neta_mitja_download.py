import os
import requests

# URL of the website containing the CSV file
url = "https://opendata-ajuntament.barcelona.cat/data/dataset/5411c8e1-1ede-47d6-92ce-2035141d8721/resource/f791aeba-2570-4e37-b957-c6036a0c28f7/download"


# Specify the local directory and file name to save the downloaded CSV
local_directory = "C:\\TFM\\webScraping\\csv_data"
local_file_name = "renda_neta_mitjana_per_persona.csv"
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

