import asyncio
from pathlib import Path
import datetime 
from datetime import timezone

from db import init_db, get_db
from app import Shop, Employee
from app.config import SHOP_ID, ADMIN_EMAIL, COURIER_EMAIL, CASHIER_1_EMAIL, CASHIER_2_EMAIL, CASHIER_3_EMAIL
from app.config import KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY, KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT
from app.config import KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC
from app.services.products_service import pay_for_products, do_delivery, get_quantity_of_updated_products
from app.services.transactions_service import make_transactions_report
from app.services.sync_service import sync_category, sync_product, update_product_desc
from kafka.producer import init_producer, close_producer, update_products_quantity_to_oltp
from kafka.consumer import consume_messages
from oltp_sync import authorize_shop, authorize_employee, authorize_terminal, get_id_available_terminals
from oltp_sync import sync_categories, sync_products
from rossmann_sync_schemas import CategorySchema, ProductSchema, ProductDescSchema


START_DATE_TIME = datetime.datetime(2025, 6, 1, 8, 0)
TIME_STEP = 0.01
MINUTES_STEP = 5
TIME_KAFKA_SEND_UPDATE_TO_OLTP = 10
current_day = 1
product_price_factor = 1.0


shop: Shop = None
admin: Employee = None
courier: Employee = None


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
    global current_day, product_price_factor
    init_db()
    asyncio.create_task(start_time())
    asyncio.create_task(start_kafka_producer())
    asyncio.create_task(consume_messages({KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY: sync_category_by_kafka}))
    asyncio.create_task(consume_messages({KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT: sync_product_by_kafka}))
    asyncio.create_task(consume_messages({KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC: update_product_desc_by_kafka}))
    for current_day in range(current_day, 61):  
        print(datetime.datetime.now())
        if current_day % 20 == 0:
            product_price_factor += 0.05
        await start_work_shift()
        
        
async def start_kafka_producer():
    print('producrer started')
    try:
        await init_producer()  
        while True:
            db = next(get_db())
            try:
                updated_items = get_quantity_of_updated_products(db)
                print(f"Updated items: {updated_items}")
                if updated_items:
                    await update_products_quantity_to_oltp(updated_items)
            finally:
                db.close()
            await asyncio.sleep(TIME_KAFKA_SEND_UPDATE_TO_OLTP)
    finally:
        await close_producer()
        print("Kafka producer closed.")
    
async def start_work_shift():    
    global shop, admin, courier, working_time
    
    while datetime.datetime.now().hour < 9:
        await asyncio.sleep(TIME_STEP)
    
    shop = authorize_shop(SHOP_ID, 'password')
    if shop:
        print(f"Shop {shop.shop_id} is authorized.")
    else:
        print(f"Authorization {shop.shop_id} failed.")
        return
    
    admin = authorize_employee(ADMIN_EMAIL, 'password')
    if admin:
        print(f"Admin {admin.first_name} is authorized.")
    else:
        print(f"Authorization {admin.first_name} failed.")
        return
    
    courier = authorize_employee(COURIER_EMAIL, 'password')
    if courier:
        print(f"Courier {courier.first_name} is authorized.")
    else:
        print(f"Authorization {courier.first_name} failed.")
        return
    
    terminals = get_id_available_terminals(shop.shop_id)
    if terminals:
        for terminal in terminals:
            if not authorize_terminal(shop.shop_id, terminal, 'password'):
                print(f"Authorization terminal {terminal} failed.")
                return
            print(f"Terminal {terminal} is authorized.")
    
    print(f'Work is started {datetime.datetime.now()}')    
    sync()
    
    task_delivery = asyncio.create_task(start_delivery_process())
    task1 = asyncio.create_task(start_cashier_working(terminals[0], CASHIER_1_EMAIL, 'password'))
    task2 = asyncio.create_task(start_cashier_working(terminals[1], CASHIER_2_EMAIL, 'password'))
    task3 = asyncio.create_task(start_cashier_working(terminals[2], CASHIER_3_EMAIL, 'password'))
    
    await asyncio.gather(task_delivery, task1, task2, task3)
    print(f'Work is ended {datetime.datetime.now()}')
    db = next(get_db())
    try:
        make_transactions_report(db, shop, admin)
    finally:
        db.close()
    datetime.datetime.add_minutes(11 * 60)
    

async def start_time():
    while True:
        await asyncio.sleep(TIME_STEP)
        datetime.datetime.add_minutes(MINUTES_STEP)
        
async def start_delivery_process():
    while 9 <= datetime.datetime.now().hour < 21:
        now = datetime.datetime.now()
        if (now.hour, now.minute) in [(10, 0), (18, 0)]:
            db = next(get_db())
            try:
                do_delivery(db, shop, admin, courier)
            finally:
                db.close()
        await asyncio.sleep(TIME_STEP)
    
async def start_cashier_working(terminal_id, email: str, password: str):
    employee = authorize_employee(email, password)
    if employee:
        print(f"Employee {employee.first_name} is authorized. Terminal ID: {terminal_id}")
    else:
        print(f"Authorization {shop.shop_id} failed.")
        return
    
    while 9 <= datetime.datetime.now().hour < 21:
        db = next(get_db())
        try:
            pay_for_products(db, terminal_id, employee, factor=product_price_factor)
        finally:
            db.close()
        await asyncio.sleep(TIME_STEP)
        

async def sync_category_by_kafka(value: str, updated_at_utc: datetime):
    try:
        category = CategorySchema.model_validate_json(value)  
    except Exception as e:
        print(f"Error parsing category data: {e}")
        return
    sync_time = get_sync_time()
    
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
    finally:
        db.close()  
        
async def sync_product_by_kafka(value: str, updated_at_utc: datetime):
    try:
        product = ProductSchema.model_validate_json(value)  
    except Exception as e:
        print(f"Error parsing product data: {e}")
        return
    sync_time = get_sync_time()
    
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
    finally:
        db.close()
        
async def update_product_desc_by_kafka(value: str, updated_at_utc: datetime):
    try:
        product_desc = ProductDescSchema.model_validate_json(value)  
    except Exception as e:
        print(f"Error parsing product data: {e}")
        return
    sync_time = get_sync_time()
    
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
            barcode=product_desc.barcode
        )
    finally:
        db.close()
        

def sync():
    sync_time = get_sync_time()
        
    db = next(get_db())
    try:
        sync_time = sync_time.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
        sync_categories(db, sync_time)
        sync_products(db, shop.shop_id, sync_time)
        print("Synchronized successfully.")
    finally:
        db.close()
        
def get_sync_time() -> 'original_datetime':
    path = Path(f"sync_time_{shop.shop_id}")
    if path.exists():
        sync_time_str = path.read_text()
        sync_time = original_datetime.fromisoformat(sync_time_str.replace('Z', '+00:00'))
    else:
        sync_time = original_datetime.min.replace(tzinfo=timezone.utc)
    
    current_time = original_datetime.now(tz=timezone.utc)
    path.write_text(current_time.isoformat().replace('+00:00', 'Z'))
    return sync_time
        
    
if __name__ == "__main__":
    asyncio.run(main())
