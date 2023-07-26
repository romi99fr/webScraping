import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import json
from selenium.webdriver.chrome.options import Options

# Configurar Selenium y abrir el navegador
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36")
driver = webdriver.Chrome(options=chrome_options)

# Número total de páginas
total_pages = 62

# Lista para almacenar los datos de todos los productos
all_product_data = []
visited_products = set()

for page in range(1, total_pages + 1):
    # Construir la URL de la página actual
    page_url = f"https://www.yaencontre.com/venta/garajes/barcelona/pag-{page}"
    driver.get(page_url)

    # Esperar a que se carguen los productos adicionales
    time.sleep(6)
    scroll_pause_time = 1  # Tiempo de pausa entre cada desplazamiento del scroll
    screen_height = driver.execute_script("return window.innerHeight")
    i = 1
    while True:
        # Desplazar el scroll hasta el final de la página
        driver.execute_script(f"window.scrollTo(0, {screen_height * i});")
        i += 5
        time.sleep(scroll_pause_time)
        # Obtener el contenido HTML de la página completa después de cargar todos los datos
        html_content = driver.page_source
        # Encontrar todos los elementos de producto
        product_containers = driver.find_elements(By.CSS_SELECTOR, 'div.content')

        # Obtener datos de los productos en la página actual
        product_data = []
        for container in product_containers:
            product_link = container.find_element(By.CSS_SELECTOR, 'a.d-ellipsis').get_attribute('href')
            if product_link in visited_products:
                continue  # Saltar a la siguiente iteración si el producto ya ha sido visitado

            visited_products.add(product_link)
            title = container.find_element(By.CSS_SELECTOR, 'a.d-ellipsis').text.strip()
            size = container.find_element(By.CSS_SELECTOR, 'div.iconGroup').text.strip()
            price = container.find_element(By.CSS_SELECTOR, 'span.price').text.strip()
            
            product_data.append({
                'title': title,
                'size': size,
                'price': price,
                'link': product_link
            })

        # Agregar los datos de los productos de la página actual a la lista general
        all_product_data.extend(product_data)
        if i >= 25:
            break

# Guardar los datos en un archivo JSON
filename = "./data/data2.json"
with open(filename, mode='w', encoding='utf-8') as file:
    json.dump(all_product_data, file, ensure_ascii=False)

print(f"Los datos se han guardado correctamente en el archivo {filename}.")
