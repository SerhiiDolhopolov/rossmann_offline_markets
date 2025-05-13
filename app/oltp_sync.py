import requests

from sqlalchemy.orm import Session

from app import Employee
from app import Shop
from app.services.sync_service import sync_category, sync_product
from app.config import OLTP_API_HOST, OLTP_API_PORT


API_URL = 'http://{OLTP_HOST}:{OLTP_PORT}'.format(OLTP_HOST=OLTP_API_HOST, 
                                                  OLTP_PORT=OLTP_API_PORT)

def authorize_shop(shop_id: int, password: str) -> Shop | None:
    url = '{API_URL}/shops/authorize?shop_id={shop_id}&password={password}'.format(
        API_URL=API_URL, 
        shop_id=shop_id, 
        password=password
    )
    response = requests.get(url)
    json_response = response.json()
    try:
        shop = Shop(shop_id=json_response['shop_id'],
                    city_name=json_response['city_name'],
                    country_name=json_response['country_name'])
    except:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        shop = None
    return shop

def get_id_available_terminals(shop_id: int) -> list[int] | None:
    url = '{API_URL}/terminals/get_available?shop_id={shop_id}'.format(
        API_URL=API_URL, 
        shop_id=shop_id
    )
    response = requests.get(url)
    json_response = response.json()
    
    if response.status_code != 200:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        return None
    return json_response

def authorize_terminal(shop_id: int, terminal_id: int, password: str) -> Shop | None:
    url = '{API_URL}/terminals/authorize?shop_id={shop_id}&terminal_id={terminal_id}&password={password}'.format(
        API_URL=API_URL, 
        shop_id=shop_id, 
        terminal_id=terminal_id,
        password=password
    )
    response = requests.get(url)
    json_response = response.json()
    
    if response.status_code != 200 or json_response == False:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        return None
    return True

def authorize_employee(email: str, password: str) -> Employee | None:
    url = '{API_URL}/employees/authorize?email={email}&password={password}'.format(
        API_URL=API_URL, 
        email=email, 
        password=password
    )
    response = requests.get(url)
    json_response = response.json()
    try:
        employee = Employee(employee_id=json_response['employee_id'],
                            first_name=json_response['first_name'],
                            last_name=json_response['last_name'],
                            role=json_response['role'])
    except:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        employee = None
    return employee

def sync_categories(db: Session, sync_utc_time: str):
    url = '{API_URL}/categories/sync?sync_utc_time={sync_utc_time}'.format(
        API_URL=API_URL,
        sync_utc_time=sync_utc_time
    )
    response = requests.get(url)
    json_response = response.json()
    if response.status_code != 200:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        return None
    
    for category in json_response:
        sync_category(db, 
                      is_deleted=category['is_deleted'],
                      category_id=category['category_id'],
                      name=category['name'],
                      description=category['description'])

def sync_products(db: Session, shop_id: int, sync_utc_time: str):
    url = '{API_URL}/products/sync?shop_id={shop_id}&sync_utc_time={sync_utc_time}'.format(
        API_URL=API_URL,
        shop_id=shop_id,
        sync_utc_time=sync_utc_time
    )
    response = requests.get(url)
    json_response = response.json()
    
    if response.status_code != 200:
        print(f"Error! Response answer: {response.status_code}")
        print(f"Response: {json_response}")
        return None
    
    for product in json_response:
        sync_product(db, 
                     is_deleted=product['is_deleted'],
                     product_id=product['product_id'],
                     name=product['name'],
                     description=product['description'],
                     barcode=product['barcode'],
                     category_id=product['category_id'],
                     price=product['price'],
                     discount=product['discount'])