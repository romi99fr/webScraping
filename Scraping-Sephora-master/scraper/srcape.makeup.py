import requests
import os
import re

# Get Response of "brandlist" Website from Sephora
makeup_lst_link = "https://www.sephora.com/shop/makeup-cosmetics"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}
response = requests.get(makeup_lst_link, headers=headers)
print(response.content)

# Extract makeup names and links using regular expressions
makeup_links = re.findall(r'"titleText":"([^"]+)"', response.text)
makeup_names = [makeup.replace("\\u0026", "&") for makeup in makeup_links]

# Check if brand is already in the file
brand_name_file_path = os.path.join('data', 'brand_names.txt')
existing_makeup_names = []
if os.path.exists(brand_name_file_path):
    with open(brand_name_file_path, 'r') as f:
        existing_makeup_names = f.read().splitlines()

# Filter makeup names and links based on brand existence
filtered_makeup_names = []
filtered_makeup_links = []
for name, link in zip(makeup_names, makeup_links):
    if name in existing_makeup_names:
        filtered_makeup_names.append(name)
        filtered_makeup_links.append(f"https://www.sephora.com/shop/makeup-cosmetics/{link}")
# Create the 'data' directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Write brand links into a file:
file_path = os.path.join('data', 'makeup_names.txt')
with open(file_path, 'w') as f:
    for item in filtered_makeup_names:
        f.write(f"{item}\n")

# Write makeup links into a file:
file_path = os.path.join('data', 'makeup_links.txt')
with open(file_path, 'w') as f:
    for item in filtered_makeup_links:
        f.write(f"{item}\n")

# Indicate scraping completion
print(f'Got All Makeups Links! There are {len(makeup_names)} brands in total.')