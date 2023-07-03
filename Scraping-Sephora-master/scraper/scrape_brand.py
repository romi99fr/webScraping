import requests
import os
import re

# Get Response of "brandlist" Website from Sephora
brand_lst_link = "https://www.sephora.com/brands-list"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}
response = requests.get(brand_lst_link, headers=headers)


brand_links = re.findall(r'"targetUrl":"/brand/([^"]+)"', response.text)
brand_names = [re.sub(r'^.*?/', '', link) for link in brand_links]
brand_urls = [f"https://www.sephora.com/brand/{link}" for link in brand_links]

# Sort brand names alphabetically
brand_names_sorted = sorted(brand_names)

# Remove entries that consist only of numbers
brand_names_filtered = [name for name in brand_names_sorted if not name.isdigit()]

# Remove duplicate brand names while preserving the order
unique_brand_names = []
seen = set()
for name in brand_names_filtered:
    if name not in seen:
        unique_brand_names.append(name)
        seen.add(name)

# Create the 'data' directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Write sorted and unique brand names into a file:
names_file_path = os.path.join('data', 'brand_names.txt')
with open(names_file_path, 'w') as f:
    for item in unique_brand_names:
        f.write(f"{item}\n")

# Write brand links corresponding to the sorted and unique brand names into a file
links_file_path = os.path.join('data', 'brand_links.txt')
with open(links_file_path, 'w') as f:
    for name in unique_brand_names:
        index = brand_names.index(name)
        f.write(f"{brand_urls[index]}\n")

# Indicate scraping completion
print(f'Got All Unique Brand Names and Links! There are {len(unique_brand_names)} brands in total.')
