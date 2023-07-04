import time
import requests
from bs4 import BeautifulSoup

# Obtener el contenido HTML de la página
brand_lst_link = "https://www.sephora.com/shop/skincare?currentPage={}"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}

product_data = []

# Número total de páginas
total_pages = 400
i = 1
for page in range(1, total_pages + 1):
    print(page)
    # Construir la URL de la página actual
    page_url = brand_lst_link.format(page)

    response = requests.get(page_url, headers=headers)
    html_content = response.content

    # Crear el objeto BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
  
    product_containers = soup.find_all('a', class_='css-klx76')
      
    # Iterate over each product container
    for tile in product_containers:
        product_info = {}
        if tile.find("span", class_="ProductTile-name"):
            product_name = tile.find("span", class_="ProductTile-name").text.strip()
            if product_name:
                product_info["name"] = product_name

        if tile.find("span", class_="css-12z2u5"):
            brand = tile.find("span", class_="css-12z2u5").text.strip()
            if brand:
                product_info["brand"] = brand

        rating_element = tile.find("span", class_="css-mu0xdx")
        if rating_element:
            rating = rating_element["aria-label"].split()[0]  # Extract the rating value
            if rating:
                product_info["rating"] = rating

        if tile.find("span", class_="css-qbbayi"):
            review_count = tile.find("span", class_="css-qbbayi").text.strip()
            if review_count:
                product_info["review_count"] = review_count

        if tile.find("b", class_="css-1f35s9q"):
            price = tile.find("b", class_="css-1f35s9q").text.strip()
            if price:
                product_info["price"] = price
        
        if product_info not in product_data:
            product_data.append(product_info)

            # Save the data to a text file
            with open('skincare_data.txt', 'a') as file:
                file.write(str(product_info) + '\n')
           
time.sleep(1)
