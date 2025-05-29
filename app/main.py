import logging
import datetime
from datetime import timezone
from pathlib import Path
import asyncio

from db import init_db, get_db
from app import Shop, Employee
from app.config import init_logging
from app.config import SHOP_ID, ADMIN_EMAIL, COURIER_EMAIL
from app.config import CASHIER_1_EMAIL, CASHIER_2_EMAIL, CASHIER_3_EMAIL
from app.config import KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY, KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT
from app.config import KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC
from app.services.products_service import (
    pay_for_products,
    do_delivery,
    get_quantity_of_updated_products,
)
from app.services.transactions_service import make_transactions_report
from app.services.sync_service import sync_category, sync_product, update_product_desc
from kafka.producer import init_producer, close_producer, update_products_quantity_to_oltp
from kafka.consumer import consume_messages
from oltp_sync import authorize_shop, authorize_employee, authorize_terminal
from oltp_sync import get_id_available_terminals
from oltp_sync import sync_categories, sync_products
from rossmann_sync_schemas import CategorySchema, ProductSchema, ProductDescSchema

init_logging()
logger = logging.getLogger(__name__)

START_DATE_TIME = datetime.datetime(2025, 3, 1, 8, 0)
TIME_STEP = 0.1
MINUTES_STEP = 5
TIME_KAFKA_SEND_UPDATE_TO_OLTP = 10
DAYS = 60
DELIVERY_COUNT_FROM = 200
DELIVERY_COUNT_TO = 500

#For Big Data
# START_DATE_TIME = datetime.datetime(2024, 9, 1, 8, 0)
# TIME_STEP = 0.0001
# MINUTES_STEP = 0.05
# TIME_KAFKA_SEND_UPDATE_TO_OLTP = 100
# DAYS = 300
# DELIVERY_COUNT_FROM = 5000
# DELIVERY_COUNT_TO = 10000


class PatchedDateTime(datetime.datetime):
    _fake_now = START_DATE_TIME

    @classmethod
    def now(cls, tz=None):
        return cls._fake_now

    @classmethod
    def add_minutes(cls, minutes):
        cls._fake_now += datetime.timedelta(minutes=minutes)


original_datetime = datetime.datetime
datetime.datetime = PatchedDateTime


async def main():
    updated_products_id = set()
    init_db()
    asyncio.create_task(start_time())
    asyncio.create_task(start_kafka_producer(updated_products_id))
    asyncio.create_task(
        consume_messages(
            {KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY: sync_category_by_kafka}
        )
    )
    asyncio.create_task(
        
        consume_messages(
            {KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT: sync_product_by_kafka}
        )
    )
    asyncio.create_task(
        consume_messages(
            {KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC: update_product_desc_by_kafka}
        )
    )

    product_price_factor = 1.0
    for current_day in range(1, DAYS + 1):
        if current_day % 20 == 0:
            product_price_factor += 0.05
        await start_work_shift(updated_products_id, product_price_factor)


async def start_kafka_producer(updated_products_id: set[int]):
    logger.info("Starting Kafka producer...")
    try:
        producer = await init_producer()
        while True:
            db = next(get_db())
            try:
                updated_products = get_quantity_of_updated_products(db, updated_products_id)
                logger.debug(f"Updated products: {updated_products}")
                if updated_products:
                    producer = await update_products_quantity_to_oltp(producer, updated_products)
            finally:
                db.close()
            await asyncio.sleep(TIME_KAFKA_SEND_UPDATE_TO_OLTP)
    finally:
        await close_producer(producer)
        logger.info("Kafka producer closed.")


async def start_work_shift(
    updated_products_id: set[int], 
    product_price_factor: float = 1.0
):
    while datetime.datetime.now().hour < 9:
        await asyncio.sleep(TIME_STEP)

    shop = authorize_shop(SHOP_ID, "password")
    if shop:
        logger.info(f"Shop %s is authorized.", SHOP_ID)
    else:
        logger.error(f"Authorization %s failed.", SHOP_ID)
        return

    admin = authorize_employee(ADMIN_EMAIL, "password")
    if admin:
        logger.info(f"Admin %s is authorized.", ADMIN_EMAIL)
    else:
        logger.error(f"Authorization %s failed.", ADMIN_EMAIL)
        return

    courier = authorize_employee(COURIER_EMAIL, "password")
    if courier:
        logger.info(f"Courier %s is authorized.", COURIER_EMAIL)
    else:
        logger.error(f"Authorization %s failed.", COURIER_EMAIL)
        return

    terminals = get_id_available_terminals(shop.shop_id)
    if terminals:
        for terminal in terminals:
            if not authorize_terminal(shop.shop_id, terminal, "password"):
                logger.error(f"Authorization terminal %s failed.", terminal)
                return
            logger.info(f"Terminal %s is authorized.", terminal)
    else:
        logger.error("No available terminals found for the shop.")
        return
    logger.info("Work is started %s", datetime.datetime.now())
    sync(shop)

    task_delivery = asyncio.create_task(
        start_delivery_process(
            shop,
            admin,
            courier,
            updated_products_id,
        )
    )
    
    task_cashier_1, task_cashier_2, task_cashier_3 = tuple(
        asyncio.create_task(
            start_cashier_working(
                terminal,
                email,
                "password",
                updated_products_id=updated_products_id,
                product_price_factor=product_price_factor
            )
        )
        for terminal, email in zip(terminals, [
            CASHIER_1_EMAIL, CASHIER_2_EMAIL, CASHIER_3_EMAIL
        ])
    )

    await asyncio.gather(
        task_delivery, 
        task_cashier_1, 
        task_cashier_2, 
        task_cashier_3
    )
    logger.info("Work is ended %s", datetime.datetime.now())
    
    db = next(get_db())
    try:
        await make_transactions_report(db, shop, admin)
    finally:
        db.close()
    datetime.datetime.add_minutes(11 * 60)


async def start_time():
    while True:
        await asyncio.sleep(TIME_STEP)
        datetime.datetime.add_minutes(MINUTES_STEP)


async def start_delivery_process(
    shop: Shop,
    admin: Employee,
    courier: Employee,
    updated_products_id: set[int],
):
    while 9 <= datetime.datetime.now().hour < 21:
        now = datetime.datetime.now()
        if (now.hour, now.minute) in [(10, 0), (18, 0)]:
            db = next(get_db())
            try:
                await do_delivery(
                    db=db, 
                    shop=shop, 
                    admin=admin, 
                    courier=courier, 
                    updated_products_id=updated_products_id,
                    count_from=DELIVERY_COUNT_FROM,
                    count_to=DELIVERY_COUNT_TO,)
            finally:
                db.close()
        await asyncio.sleep(TIME_STEP)


async def start_cashier_working(
    terminal_id,
    email: str,
    password: str,
    updated_products_id: set[int], 
    product_price_factor: float = 1.0
):
    employee = authorize_employee(email, password)
    if employee:
        logger.info("Employee %s is authorized.", email)
    else:
        logger.error("Authorization %s failed.", email)
        return

    while 9 <= datetime.datetime.now().hour < 21:
        db = next(get_db())
        try:
            pay_for_products(
                db=db,
                terminal_id=terminal_id,
                cashier=employee,
                updated_products_id=updated_products_id,
                factor=product_price_factor
            )
        finally:
            db.close()
        await asyncio.sleep(TIME_STEP)


async def sync_category_by_kafka(value: str):
    try:
        category = CategorySchema.model_validate_json(value)
    except Exception as e:
        logger.error(f"Error parsing category data: %s", e, exc_info=True)
        return
    
    sync_time = get_sync_time()
    updated_at_utc = category.updated_at_utc

    if updated_at_utc <= sync_time:
        return

    db = next(get_db())
    try:
        sync_category(
            db,
            is_deleted=category.is_deleted,
            category_id=category.category_id,
            name=category.name,
            description=category.description,
        )
        update_sync_time(updated_at_utc)
        logger.info(
            "Category updated successfully for category ID %s",
            category.category_id,
        )
    finally:
        db.close()


async def sync_product_by_kafka(value: str):
    try:
        product = ProductSchema.model_validate_json(value)
    except Exception as e:
        logger.error(f"Error parsing product data: %s", e, exc_info=True)
        return
    
    sync_time = get_sync_time()
    updated_at_utc = product.updated_at_utc

    if updated_at_utc <= sync_time:
        return

    db = next(get_db())
    try:
        sync_product(
            db,
            is_deleted=product.is_deleted,
            product_id=product.product_id,
            name=product.name,
            description=product.description,
            barcode=product.barcode,
            category_id=product.category_id,
            price=product.price,
            discount=product.discount,
        )
        update_sync_time(updated_at_utc)
        logger.info(
            "Product updated successfully for product ID %s",
            product.product_id,
        )
    finally:
        db.close()


async def update_product_desc_by_kafka(value: str):
    try:
        product_desc = ProductDescSchema.model_validate_json(value)
    except Exception as e:
        logger.error(f"Error parsing product description data: %s", e, exc_info=True)
        return
    sync_time = get_sync_time()

    updated_at_utc = product_desc.updated_at_utc
    if updated_at_utc <= sync_time:
        return

    db = next(get_db())
    try:
        update_product_desc(
            db,
            is_deleted=product_desc.is_deleted,
            product_id=product_desc.product_id,
            name=product_desc.name,
            description=product_desc.description,
            barcode=product_desc.barcode,
        )
        update_sync_time(updated_at_utc)
        logger.info(
            "Product description updated successfully for product ID %s",
            product_desc.product_id,
        )
    finally:
        db.close()


def sync(shop):
    sync_time = get_sync_time()
    db = next(get_db())
    try:
        sync_time_str = sync_time.isoformat().replace("+00:00", "Z")
        max_updated_at_cat = sync_categories(db, sync_time_str)
        max_updated_at_prod = sync_products(db, shop.shop_id, sync_time_str)
        max_updated_at_time = max(max_updated_at_cat, max_updated_at_prod)
        if max_updated_at_time > sync_time:
            update_sync_time(max_updated_at_time)
            logger.info(
                "Sync time updated to %s",
                max_updated_at_time.isoformat(),
            )
        logger.info("Categories and products start synchronized successfully.")
    finally:
        db.close()


def get_sync_time() -> "original_datetime":
    path = Path(f"sync_time_{SHOP_ID}")
    if path.exists():
        sync_time = path.read_text()
        try:
            sync_time = original_datetime.fromisoformat(sync_time)
        except Exception as e:
            logger.error(
                "Failed to parse sync time from file %s: %s",
                path,
                e,
                exc_info=True,
            )
            sync_time = original_datetime.min.replace(tzinfo=timezone.utc)
    else:
        sync_time = (
            original_datetime
            .min
            .replace(tzinfo=timezone.utc)
        )
    return sync_time


def update_sync_time(sync_time: "original_datetime"):
    path = Path(f"sync_time_{SHOP_ID}")
    path.write_text(sync_time.isoformat())
    logger.debug("Sync time updated to %s", sync_time.isoformat())


if __name__ == "__main__":
    asyncio.run(main())
