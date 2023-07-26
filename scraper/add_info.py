import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
from selenium.webdriver.chrome.options import Options
import spacy
import re

# Configurar Selenium y abrir el navegador
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36")
driver = webdriver.Chrome(options=chrome_options)

all_product_data = []

# Cargar el archivo JSON con los datos
filename = "./data/data2.json"
with open(filename, mode='r', encoding='utf-8') as file:
    data = json.load(file)

# Recorrer cada elemento del JSON
for product_data in data:
    product_link = product_data['link']
    driver.get(product_link)
    time.sleep(3)  # Esperar para que se cargue la página completa

    # Obtener el contenido HTML de la página completa después de cargar todos los datos
    html_content = driver.page_source

    # Encontrar todos los elementos de producto
    try:
        address_element = driver.find_element(By.CSS_SELECTOR, 'div.details-address.icon-placeholder-1.mb-lg.pointer')
        address = address_element.text.strip()
    except Exception as e:
        address = ""

    # Obtener la descripción
    try:
        desc_element = driver.find_element(By.CSS_SELECTOR, 'div.raw-format.description.readMoreText')
        desc = desc_element.text.strip()
        # Eliminar los saltos de línea de la descripción
        desc = desc.replace('\n', '')
    except Exception as e:
        desc = ""

    # Distribuir la dirección en barri, districte, ciutat
    components = address.split(',')
    if len(components) >= 3:
        barri = components[0]
        districte = components[1]
        ciutat = components[2]


    # Patrón para buscar el nombre de la calle
    pattern_street = r"(?i)(?<!\w)(Garaje en(?: calle)?)(?:\s+([^,]+))?"
    pattern_door = r"(?i)(?<!\w)(?:[.,])?\s*(?:puerta(?:\s+a\s+distancia)?|mando|puerta\s+de\s+acceso\s+es\s+automática|motor|motorizada)(?:\s+(?:basculante|de acceso|mecanizada|automática|a distancia))?\b"
    pattern_floor = r"(?i)\b(?:planta\s*-?\s*(?:primera|segunda|tercera|cuarta|quinta|\d+)|primera\s*planta|segunda\s*planta|tercera\s*planta|cuarta\s*planta|quinta\s*planta|semisótano|sotano|sótano\s*-?\s*\d+|sótano\s*uno|sótano\s*dos|sótano|única\s*planta\s*sótano)\b"
    pattern_coche = r"(?i)\bcoche\s*(pequeño|mediano|grande)\b"
    pattern_gastos = r"(?i)\bGastos de impuestos de transmisiones patrimoniales, aranceles, notariales, registrales a cargo de la parte (compradora|vendedora)\b"
    pattern_columnas = r"(?i)\b(?:columnas\s*laterales|entre\s*columnas|dos\s*columnas\s*laterales|dos\s*columnas|tres\s*columnas|protegidas\s*por\s*columnas|protegidas\s*entre\s*columnas|pocas\s*columnas|columna)\b"
    pattern_no_columnas = r"(?i)\b(?:no\s*tiene\s*columnas|sin\s*columnas|sin\s*columna)\b"
    pattern_ascensor = r"(?i)\bascensores?\b"
    pattern_sin_ascensor = r"(?i)\bsin\s+ascensor\b"
    pattern_vigilancia = r"(?i)\b(vigilante|vigilancia|vigilado|videovigilado|videovigilancia)\b"
    

    # Buscar todas las coincidencias de vigilancia
    matches_vigilancia = re.findall(pattern_vigilancia, desc)
    if matches_vigilancia:
        product_data["Vigilancia"] = "Si"
    else:
        product_data["Vigilancia"] = "No"


    # Buscar si hay ascensor
    matches_ascensor = re.findall(pattern_ascensor, desc)
    matches_noascensor = re.findall(pattern_sin_ascensor, desc)
    atributo_ascensor = "Si" if matches_ascensor else "No" if matches_noascensor else "No info"
    if matches_ascensor:
        product_data["Ascensor"] = atributo_ascensor


    # Buscar si hay o no columnas
    matches = re.findall(pattern_columnas, desc)
    matches_no_columnas = re.findall(pattern_no_columnas, desc)
    atributo_columnas = "Si" if matches else "No" if matches_no_columnas else "No info"
    if atributo_columnas:
        product_data["Columnas"] = atributo_columnas   


    # Buscar si hay gastos a cargo de comprador o vendedor
    gastos_matches = re.findall(pattern_gastos, desc)
    if gastos_matches:
        gasto = gastos_matches[0]
        product_data["Gastos a cargo de"] = gasto

    # Buscar el tamaño del coche
    size_car = re.findall(pattern_coche, desc)
    size_low = [tamano.lower() for tamano in size_car]
    if size_low:
        size = size_low[0]
        product_data["Tipo coche"] = size

    # Buscar coincidencias de la calle
    matches_street = re.findall(pattern_street, product_data["title"])
    if matches_street:
        street_name = matches_street[0][1]

    # Buscar y capturar el tipo de puerta
    description = re.findall(pattern_door, desc)
    if description:
        door = description[0]
        product_data["Puerta"] = "automática"

    # Buscar y capturar la planta
    floor = re.findall(pattern_floor, desc)
    if floor:
        floor_info = floor[0]
        product_data["Planta"] = floor_info

    def extract_measures_with_nlp(text):
        # Cargar el modelo de lenguaje en español de spaCy
        nlp = spacy.load('es_core_news_sm')

        # Procesar el texto con spaCy
        doc = nlp(text)

        # Patrones para buscar medidas en el texto
        patterns = [
            r"largo:\s+(\d+\.\d+)\s+ancho:\s+(\d+\.\d+)",
            r"ancho\s+(\d+\,\d+)\s+largo\s+(\d+\,\d+)",
            r"(\d+\,\d+)\s+ANCHO.*?(\d+\,\d+)\s+LARGO",
            r"(?:(\d+\.\d+)m\s*x\s*(\d+\.\d+)m)",
            r"(\d+\.\d+)\s*x\s*(\d+\.\d+)",
            r"(\d+(?:,\d+)?) m por (\d+(?:,\d+)?) m",
            r"(\d+(?:,\d+)?)m de largo x (\d+(?:,\d+)?)m de ancho",
            r"(\d+(?:,\d+)?)\s*x\s*(\d+(?:,\d+)?)",
            r"(\d+(?:\.\d+)?)\s*m\.? ancho x (\d+(?:\.\d+)?)\s*m\.? fondo",
            r"(\d+(?:,\d+)?)\s*X\s*(\d+(?:,\d+)?)",
            r"(\d+(?:\.\d+)?)\s*de\s*largo\s*y\s*(\d+(?:\.\d+)?)\s*de\s*ancho",
            r"(\d+(?:,\d+)?)m \(columna\) x (\d+(?:,\d+)?)m"
            r"\((\d+(?:\.\d+)?)mt de ancho por (\d+(?:\.\d+)?)mt de ancho\)",
            r"(\d+(?:,\d+)?) metros de ancho por (\d+(?:,\d+)?) metros de largo",
            r"(\d+(?:\.\d+)?)m largo x (\d+(?:\.\d+)?)m ancho",
            r"(\d+(?:,\d+)?) m de ancho por (\d+(?:,\d+)?) m de largo",
            r"(\d+(?:\.\d+)?) mts de profundidad;\s*(\d+(?:,\d+)?) mts de ancho",
            r"anchura después de la columna: (\d+(?:,\d+)?) m\. Longitud (\d+(?:,\d+)?) m\."
        ]

        # Buscar patrones en el texto y extraer medidas
        measures = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                measures.extend(matches[0])
                break
        
        return measures

    measures = extract_measures_with_nlp(desc)
    if measures:
        product_data["Medidas"] = measures

    # Agregar barri, districte y ciutat dentro de la variable 'address'
    address = {
        "Calle": street_name,
        "Barrio": barri,
        "Distrito": districte,
        "Ciudad": ciutat
    }
   
    # Actualizar la dirección en el producto_data
    product_data['Address'] = address
    product_data['Description'] = desc

    # Agregar los datos de los productos de la página actual a la lista general
    all_product_data.append(product_data)

# Guardar los datos actualizados en el archivo JSON
with open(filename, mode='w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False)

print(f"Los datos se han actualizado correctamente en el archivo {filename}.")
