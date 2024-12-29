import psycopg2
import requests
import re
from typing import Optional, Dict

from psycopg2._psycopg import connection

# ---------------------- CONSTANTS ---------------------- #
host = "localhost"
port = "5432"
database = "postgres"
username = "postgres"
password = "13721372"
search_url_template = "https://api.digikala.com/v1/categories/mobile-phone/brands/samsung/search/?page={0}"
get_product_url_template = "https://api.digikala.com/v2/product/{0}/"


# ---------------------- TYPES ---------------------- #

class ProductInfo:
    def __init__(self, id: int, product_name: str, weight: int, screen_size: float, pixel_density: int,
                 processor_frequency: float, internal_storage_size: int, memory_size: int, support_5g: bool,
                 camera_resolution: int, battery_capacity: int, price: int):
        self.id = id
        self.product_name = product_name
        self.weight = weight
        self.screen_size = screen_size
        self.pixel_density = pixel_density
        self.processor_frequency = processor_frequency
        self.internal_storage_size = internal_storage_size
        self.memory_size = memory_size
        self.support_5g = support_5g
        self.camera_resolution = camera_resolution
        self.battery_capacity = battery_capacity
        self.price = price

    def __changes__(self, other) -> dict:
        if other:
            if isinstance(other, ProductInfo):
                changes = {}
                if other.product_name != self.product_name:
                    changes["product_name"] = self.product_name
                if other.weight != self.weight:
                    changes["weight"] = self.weight
                if other.screen_size != self.screen_size:
                    changes["screen_size"] = self.screen_size
                if other.pixel_density != self.pixel_density:
                    changes["pixel_density"] = self.pixel_density
                if other.processor_frequency != self.processor_frequency:
                    changes["processor_frequency"] = self.processor_frequency
                if other.internal_storage_size != self.internal_storage_size:
                    changes["internal_storage_size"] = self.internal_storage_size
                if other.memory_size != self.memory_size:
                    changes["memory_size"] = self.memory_size
                if other.support_5g != self.support_5g:
                    changes["support_5g"] = self.support_5g
                if other.camera_resolution != self.camera_resolution:
                    changes["camera_resolution"] = self.camera_resolution
                if other.battery_capacity != self.battery_capacity:
                    changes["battery_capacity"] = self.battery_capacity
                if other.price != self.price:
                    changes["price"] = self.price

                return changes

            return NotImplemented
        else:
            return NotImplemented


# ---------------------- DATABASE ---------------------- #

def init_table():
    try:
        conn = psycopg2.connect(host=host, database=database, port=port, user=username, password=password)
        cursor = conn.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS PRODUCT_INFO (
            ID BIGINT PRIMARY KEY,
            PRODUCT_NAME VARCHAR(512),
            WEIGHT BIGINT,
            SCREEN_SIZE FLOAT4,
            PIXEL_DENSITY BIGINT,
            PROCESSOR_FREQUENCY FLOAT4,
            INTERNAL_STORAGE_SIZE BIGINT,
            MEMORY_SIZE BIGINT,
            SUPPORT_5G BOOLEAN,
            CAMERA_RESOLUTION BIGINT,
            BATTERY_CAPACITY BIGINT,
            PRICE BIGINT
        )
        """
        cursor.execute(create_table_sql)

        conn.commit()
        cursor.close()
        conn.close()
        print("Product info table initialized and table ensured.")
    except Exception as e:
        print(f"Error while initializing database: {e}")


def insert_product_info(product_info: ProductInfo, conn: connection):
    cursor = conn.cursor()
    cursor.execute("""
                    INSERT INTO PRODUCT_INFO (
                        id, product_name, weight, screen_size, pixel_density, processor_frequency,
                        internal_storage_size, memory_size, support_5g, camera_resolution,
                        battery_capacity, price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
        product_info.id, product_info.product_name, product_info.weight, product_info.screen_size,
        product_info.pixel_density,
        product_info.processor_frequency, product_info.internal_storage_size, product_info.memory_size,
        product_info.support_5g, product_info.camera_resolution, product_info.battery_capacity,
        product_info.price))
    conn.commit()
    cursor.close()
    print(f"Product info for ID {product_info.id} saved successfully.")


def update_product_info(product_info: ProductInfo, old_product_info: ProductInfo, conn: connection):
    changes = product_info.__changes__(old_product_info)
    if changes and len(changes) > 0:
        set_clause = ",".join([f"{column} = %s" for column in changes.keys()])
        column_values = list(changes.values())
        column_values.append(product_info.id)
        update_query = f"UPDATE PRODUCT_INFO SET {set_clause} WHERE id = %s"
        cursor = conn.cursor()
        cursor.execute(update_query, column_values)
        print(f"Product info for ID {product_info.id} updated successfully.")
        conn.commit()
        cursor.close()
    else:
        print(f"No change detect for product info with ID {product_info.id} ")


def save_product_info(product_info: ProductInfo):
    try:
        conn = psycopg2.connect(host=host, database=database, port=port, user=username, password=password)
        cursor = conn.cursor()
        cursor.execute("""
        SELECT id, product_name, weight, screen_size, pixel_density, processor_frequency,
                        internal_storage_size, memory_size, support_5g, camera_resolution,
                        battery_capacity, price FROM PRODUCT_INFO WHERE id = %s
                        """, (product_info.id,))
        existing_product = cursor.fetchone()
        if existing_product:
            old_product_info = ProductInfo(*existing_product)
            update_product_info(product_info, old_product_info, conn)
        else:
            insert_product_info(product_info, conn)
        cursor.close()
    except Exception as e:
        print(f"Error in save product info: {e}")
        raise e


def init_database():
    init_table()


# ---------------------- UTILS ---------------------- #


def extract_number(input_string: str) -> Optional[int]:
    try:
        match = re.search(r'\d+', input_string)
        if match:
            return int(match.group(0))
        else:
            return None
    except Exception as e:
        print(f"Error in extract number from string : {input_string}, reason {e}")
        return None


def extract_float(input_string: str) -> Optional[float]:
    try:
        match = re.search(r'\d+\.\d+', input_string)
        if match:
            return float(match.group(0))
        else:
            return None
    except Exception as e:
        print(f"Error in extract number from string : {input_string}, reason {e}")
        return None


# ---------------------- HTTP ---------------------- #


def fetch_data_from_url(url: str) -> Dict:
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Service not available")
    except Exception as e:
        print(f"Error in fetch data from url: {url}, reason {e}")
        raise


def fetch_products_data_by_search(page_no: int):
    try:
        search_url = search_url_template.format(page_no)
        search_response = fetch_data_from_url(search_url)
        search_status = search_response.get('status', int)
        if search_status == 200:
            return search_response.get('data', {'products': dict}).get('products', dict)
        else:
            return {}
    except Exception as e:
        print(f"Error in fetch_products_data_by_search for page {page_no}, reason: {e}")
        return {}


def fetch_products_info(id: str):
    try:
        get_product_url = get_product_url_template.format(id)
        info_response = fetch_data_from_url(get_product_url)
        search_status = info_response.get('status', int)
        if search_status == 200:
            return info_response.get('data', {'product': dict}).get('product', dict)
        else:
            print(f"Get product info failed with status code {search_status} for ID {id}")
            return {}
    except Exception as e:
        print(f"Error in fetch_products_info for ID {id}, reason: {e}")
        return {}


# ---------------------- FINDERS ---------------------- #


def find_min_price(product_info_response: dict):
    min_price = 0
    if len(product_info_response) > 0:
        variants = product_info_response.get('variants')
        if variants and len(variants) > 0:
            for variant in variants:
                price_info = variant.get('price')
                if price_info and len(price_info) > 0:
                    price = price_info.get('selling_price')
                    if price and (min_price < 1 or price < min_price):
                        min_price = price
    return min_price


def find_product_name(product_info_response: dict):
    if len(product_info_response) > 0:
        product_name = product_info_response.get('title_fa')
        if product_name and len(product_name) > 0:
            return product_name
    return None


def find_str_attribute_from_specifications(product_info_response: dict, specification_name, attribute_name):
    attribute_value = None
    if len(product_info_response) > 0:
        specifications = product_info_response.get('specifications')
        if specifications and len(specifications) > 0:
            for specification in specifications:
                if specification and len(specification) > 0:
                    title = specification.get('title')
                    if title and len(title) > 0 and title == specification_name:
                        attributes = specification.get('attributes')
                        if attributes and len(attributes) > 0:
                            for attribute in attributes:
                                if attribute and len(attribute) > 0 and attribute.get('title') and len(
                                        attribute.get('title')) > 0 and attribute.get('title') == attribute_name:
                                    attribute_values = attribute.get('values')
                                    if attribute_values and len(attribute_values) > 0:
                                        attribute_value = attribute_values[0]
                                        break
    return attribute_value


def find_number_attribute_from_specifications(product_info_response: dict, specification_name, attribute_name):
    attribute_value = 0
    if len(product_info_response) > 0:
        specifications = product_info_response.get('specifications')
        if specifications and len(specifications) > 0:
            for specification in specifications:
                if specification and len(specification) > 0:
                    title = specification.get('title')
                    if title and len(title) > 0 and title == specification_name:
                        attributes = specification.get('attributes')
                        if attributes and len(attributes) > 0:
                            for attribute in attributes:
                                if attribute and len(attribute) > 0 and attribute.get('title') and len(
                                        attribute.get('title')) > 0 and attribute.get('title') == attribute_name:
                                    attribute_values = attribute.get('values')
                                    if attribute_values and len(attribute_values) > 0:
                                        attribute_value = extract_number(attribute_values[0])
                                        break
    return attribute_value


def exist_attribute_from_specifications(product_info_response: dict, specification_name, attribute_name,
                                        attribute_value):
    exist = False
    if len(product_info_response) > 0:
        specifications = product_info_response.get('specifications')
        if specifications and len(specifications) > 0:
            for specification in specifications:
                if specification and len(specification) > 0:
                    title = specification.get('title')
                    if title and len(title) > 0 and title == specification_name:
                        attributes = specification.get('attributes')
                        if attributes and len(attributes) > 0:
                            for attribute in attributes:
                                if attribute and len(attribute) > 0 and attribute.get('title') and len(
                                        attribute.get('title')) > 0 and attribute.get('title') == attribute_name:
                                    attribute_values = attribute.get('values')
                                    if attribute_values and len(attribute_values) > 0:
                                        for value in attribute_values:
                                            if value and len(value) > 0 and attribute_value == str(value).replace(' ',
                                                                                                                  ''):
                                                exist = True
                                                break
    return exist


def find_float_attribute_from_specifications(product_info_response: dict, specification_name, attribute_name):
    attribute_value = 0.0
    if len(product_info_response) > 0:
        specifications = product_info_response.get('specifications')
        if specifications and len(specifications) > 0:
            for specification in specifications:
                if specification and len(specification) > 0:
                    title = specification.get('title')
                    if title and len(title) > 0 and title == specification_name:
                        attributes = specification.get('attributes')
                        if attributes and len(attributes) > 0:
                            for attribute in attributes:
                                if attribute and len(attribute) > 0 and attribute.get('title') and len(
                                        attribute.get('title')) > 0 and attribute.get('title') == attribute_name:
                                    attribute_values = attribute.get('values')
                                    if attribute_values and len(attribute_values) > 0:
                                        attribute_value = extract_float(attribute_values[0])
                                        break
    return attribute_value


def find_product_weight(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'مشخصات کلی', 'وزن')


def find_product_screen_size(product_info_response: dict):
    return find_float_attribute_from_specifications(product_info_response, 'صفحه نمایش', 'اندازه')


def find_product_pixel_density(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'صفحه نمایش', 'تراکم پیکسلی')


def find_product_processor_frequency(product_info_response: dict):
    return find_float_attribute_from_specifications(product_info_response, 'پردازنده', 'فرکانس پردازنده‌ مرکزی')


def find_product_internal_storage_size(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'حافظه', 'حافظه داخلی')


def find_product_memory_size(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'حافظه', 'مقدار RAM')


def find_product_support_5g(product_info_response: dict):
    return exist_attribute_from_specifications(product_info_response, 'ارتباطات', 'شبکه‌های مخابراتی', '5G')


def find_product_camera_resolution(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'دوربین', 'رزولوشن دوربین اصلی')


def find_product_battery_capacity(product_info_response: dict):
    return find_number_attribute_from_specifications(product_info_response, 'سایر مشخصات', 'ظرفیت باتری')


# ---------------------- LOGICS ---------------------- #


def create_product_info(id: int, product_info_response: dict) -> ProductInfo:
    min_price = find_min_price(product_info_response)
    product_name = find_product_name(product_info_response)
    weight = find_product_weight(product_info_response)
    screen_size = find_product_screen_size(product_info_response)
    pixel_density = find_product_pixel_density(product_info_response)
    processor_frequency = find_product_processor_frequency(product_info_response)
    internal_storage_size = find_product_internal_storage_size(product_info_response)
    memory_size = find_product_memory_size(product_info_response)
    support_5g = find_product_support_5g(product_info_response)
    camera_resolution = find_product_camera_resolution(product_info_response)
    battery_capacity = find_product_battery_capacity(product_info_response)
    return ProductInfo(id, product_name, weight, screen_size, pixel_density, processor_frequency,
                       internal_storage_size, memory_size, support_5g, camera_resolution, battery_capacity,
                       min_price)


def process_product_info(product: dict):
    try:
        id = product.get('id')
        product_info_response = fetch_products_info(id)
        product_info = create_product_info(id, product_info_response)
        save_product_info(product_info)
        print(f"Process product info with id {id} and name {product_info.product_name} success")
    except Exception as e:
        print(f"Error in process product info: {e}")


def run():
    print("Running application ... ")
    init_database()
    total_item = 0
    page_no = 1
    products = fetch_products_data_by_search(page_no)
    while len(products) > 0:
        try:
            for product in products:
                process_product_info(product)
            print(f"### Finish process product page {page_no} and product size {len(products)}")
            page_no += 1
            total_item += len(products)
            products = fetch_products_data_by_search(page_no)
        except Exception as e:
            print(f"Error in process product page: {e}")
    print(f"Finish running with total item: {total_item}")
