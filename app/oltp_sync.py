import requests
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app import Employee
from app import Shop
from app.services.sync_service import sync_category, sync_product
from app.config import OLTP_API_HOST, OLTP_API_PORT
from rossmann_sync_schemas import ShopSchema, EmployeeSchema
from rossmann_sync_schemas import ProductSchema, CategorySchema

logger = logging.getLogger(__name__)

API_URL = f"http://{OLTP_API_HOST}:{OLTP_API_PORT}"


def authorize_shop(shop_id: int, password: str) -> Shop | None:
    url = f"{API_URL}/shops/authorize?shop_id={shop_id}&password={password}"
    response = requests.get(url)
    json_response = response.json()
    try:
        shop_schema = ShopSchema.model_validate(json_response)
        shop = Shop(
            shop_id=shop_schema.shop_id,
            city_name=json_response["city_name"],
            country_name=json_response["country_name"],
        )
    except Exception as e:
        logger.error(
            "Failed to authorize shop with ID %d. "
            "Response: %s",
            "%s",
            shop_id,
            json_response,
            e,
        )
        shop = None
    return shop


def get_id_available_terminals(shop_id: int) -> list[int] | None:
    url = (
        f"{API_URL}/terminals/get_available?"
        f"shop_id={shop_id}"
    )
    response = requests.get(url)
    json_response = response.json()

    if response.status_code != 200:
        logger.error(
            "Failed to get available terminals for shop ID %d. "
            "Response: %s",
            shop_id,
            json_response,
        )
        return None
    return json_response


def authorize_terminal(shop_id: int, terminal_id: int, password: str) -> Shop | None:
    url = (
        f"{API_URL}/terminals/authorize?"
        f"shop_id={shop_id}&terminal_id={terminal_id}&password={password}"
    )
    response = requests.get(url)
    json_response = response.json()

    if response.status_code != 200 or json_response is False:
        logger.error(
            "Failed to authorize terminal with ID %d for shop ID %d. "
            "Response: %s",
            terminal_id,
            shop_id,
            json_response,
        )
        return None
    return True


def authorize_employee(email: str, password: str) -> Employee | None:
    url = (
        f"{API_URL}/employees/authorize?"
        f"email={email}&password={password}"
    )
    response = requests.get(url)
    json_response = response.json()
    try:
        employee_schema = EmployeeSchema.model_validate(json_response)
        employee = Employee(
            employee_id=employee_schema.employee_id,
            first_name=employee_schema.first_name,
            last_name=employee_schema.last_name,
            role=employee_schema.role,
        )
    except Exception as e:
        logger.error(
            "Failed to authorize employee with email %s. "
            "Response: %s",
            "%s",
            email,
            json_response,
            e,
        )
        employee = None
    return employee


def sync_categories(db: Session, sync_utc_isoZ_time: str) -> datetime:
    """RETURNS the max updated_at_utc of all categories"""
    url = (
        f"{API_URL}/categories/sync?"
        f"sync_utc_time={sync_utc_isoZ_time}"
    )
    response = requests.get(url)
    json_response = response.json()

    if response.status_code != 200:
        logger.error(
            "Failed to sync categories. "
            "Response: %s",
            json_response,
        )
        return None
    
    max_updated_at_utc = datetime.min.replace(tzinfo=timezone.utc)
    for category in json_response:
        category_schema = CategorySchema.model_validate(category)
        sync_category(
            db,
            is_deleted=category_schema.is_deleted,
            category_id=category_schema.category_id,
            name=category_schema.name,
            description=category_schema.description,
        )
        updated_at_utc = category_schema.updated_at_utc
        if updated_at_utc > max_updated_at_utc:
            max_updated_at_utc = updated_at_utc
    return max_updated_at_utc


def sync_products(db: Session, shop_id: int, sync_utc_isoZ_time: str) -> datetime:
    """RETURNS the max updated_at_utc of all products"""
    url = (
        f"{API_URL}/products/sync?"
        f"shop_id={shop_id}&sync_utc_time={sync_utc_isoZ_time}"
    )
    response = requests.get(url)
    json_response = response.json()

    if response.status_code != 200:
        logger.error(
            "Failed to sync products for shop ID %d. "
            "Response: %s",
            shop_id,
            json_response,
        )
        return None

    max_updated_at_utc = datetime.min.replace(tzinfo=timezone.utc)
    for product in json_response:
        product_schema = ProductSchema.model_validate(product)
        sync_product(
            db,
            is_deleted=product_schema.is_deleted,
            product_id=product_schema.product_id,
            name=product_schema.name,
            description=product_schema.description,
            barcode=product_schema.barcode,
            category_id=product_schema.category_id,
            price=product_schema.price,
            discount=product_schema.discount,
        )
        updated_at_utc = product_schema.updated_at_utc
        if updated_at_utc > max_updated_at_utc:
            max_updated_at_utc = updated_at_utc
    return max_updated_at_utc
