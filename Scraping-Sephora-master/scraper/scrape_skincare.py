import requests
from bs4 import BeautifulSoup

# Obtener el contenido HTML de la página
brand_lst_link = "https://www.sephora.com/shop/skincare"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}
response = requests.get(brand_lst_link, headers=headers)
html_content = response.content

# Crear el objeto BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

product_containers = soup.find_all('a', class_='css-klx76')

# Iterate over each product container
for container in product_containers:
    brand = container.find('span', class_="css-12z2u5 eanm77i0").text
    name = container.find('span', class_="ProductTile-name css-h8cc3p eanm77i0").text
    cost = container.find('b', class_="css-1f35s9q").text
    voting_rate = container.find('div', class_="css-1xk97ib").text

    rating_element = container.find('span', class_='css-mu0xdx')
    rating = rating_element.get('aria-label').split()[0] if rating_element else None

    # Print or save the extracted data
    print("Brand: ", brand)
    print("Product: ", name)
    print("Cost: ", cost)
    print("Rate: ", rating)
    print("Number of people who rate the product: ", voting_rate)
    print("---------------")

    # Save the data to a text file
    with open('product_data.txt', 'a') as file:
        file.write("Brand: " + brand + "\n")
        file.write("Product: " + name + "\n")
        file.write("Cost: " + cost + "\n")
        file.write("Rate: " + rating + "\n")
        file.write("Number of people who rate the product: " + voting_rate + "\n")
        file.write("---------------------\n")