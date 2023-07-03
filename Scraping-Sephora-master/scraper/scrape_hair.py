import requests
import os
import re

# Get Response of "brandlist" Website from Sephora
hair_lst_link = "https://www.sephora.com/shop/hair-products"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}
response = requests.get(hair_lst_link, headers=headers)
print(response.content)

# Extract makeup names and links using regular expressions
hair_links = re.findall(r'"titleText":"([^"]+)"', response.text)
hair_names = [makeup.replace("\\u0026", "&") for makeup in hair_links]

# Check if brand is already in the file
brand_name_file_path = os.path.join('data', 'brand_names.txt')
existing_hair_names = []
if os.path.exists(brand_name_file_path):
    with open(brand_name_file_path, 'r') as f:
        existing_hair_names = f.read().splitlines()

# Filter makeup names and links based on brand existence
filtered_hair_names = []
filtered_hair_links = []
for name, link in zip(hair_names, hair_links):
    if name in existing_hair_names:
        filtered_hair_names.append(name)
        filtered_hair_links.append(f"https://www.sephora.com/shop/hair-products/{link}")
# Create the 'data' directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Write brand links into a file:
file_path = os.path.join('data', 'hair_names.txt')
with open(file_path, 'w') as f:
    for item in filtered_hair_names:
        f.write(f"{item}\n")

# Write makeup links into a file:
file_path = os.path.join('data', 'hair_links.txt')
with open(file_path, 'w') as f:
    for item in filtered_hair_links:
        f.write(f"{item}\n")

# Indicate scraping completion
print(f'Got All Hair Products Links! There are {len(hair_names)} hair products in total.')