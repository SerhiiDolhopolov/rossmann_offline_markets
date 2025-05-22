from datetime import datetime
from contextlib import asynccontextmanager
import asyncio
import aiohttp
from aiochclient import ChClient

from app.config import CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DELIVERY_DB


clickhouse_url = 'http://{host}:{port}'.format(
    host = CLICKHOUSE_HOST,
    port = CLICKHOUSE_HTTP_PORT
)

class DeliveryReport:
    def __init__(self):
        self.order_data = []
        
    def add_order_data(self,
                       shop_id: int,
                       country: str,
                       city: str,
                       admin_id: int,
                       courier_id: int,
                       product_id: int,
                       product_name: str,
                       category_id: int,
                       category_name: str,
                       quantity: int,
                       accepted_time: datetime) -> None:
        self.order_data.append({
            'shop_id': shop_id,
            'country': country,
            'city': city,
            'admin_id': admin_id,
            'courier_id': courier_id,
            'product_id': product_id,
            'product_name': product_name,
            'category_id': category_id,
            'category_name': category_name,
            'quantity': quantity,
            'accepted_time': accepted_time.strftime("%Y-%m-%d %H:%M:%S"),     
        })
    
    async def save_report(self, client: ChClient, max_retries: int = 10):
        print("Saving report to ClickHouse...")
        self.order_data.sort(key=lambda x: (x['category_id'], x['product_id']))        
        query = '''
            INSERT INTO reports (
                shop_id, country, city, admin_id, courier_id,
                product_id, product_name, category_id, category_name,
                quantity, accepted_time
            ) VALUES
        '''
        values = [
            (
                row['shop_id'],
                row['country'],
                row['city'],
                row['admin_id'],
                row['courier_id'],
                row['product_id'],
                row['product_name'],
                row['category_id'],
                row['category_name'],
                row['quantity'],
                row['accepted_time'],
            )
            for row in self.order_data
        ]
        for attempt in range(max_retries):
            try:
                await client.execute(query, *values)
                self.order_data = []
                return True
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(600)
               
@asynccontextmanager 
async def get_delivery_client():
    session = aiohttp.ClientSession()
    client = ChClient(session, 
                      url=clickhouse_url,
                      user=CLICKHOUSE_USER, 
                      password=CLICKHOUSE_PASSWORD, 
                      database=CLICKHOUSE_DELIVERY_DB)
    print(f"Connecting to ClickHouse at {clickhouse_url}...")
    try:
        await client.execute("SELECT 1")
        print("ClickHouse connection OK")
        yield client
    finally:
        await session.close()