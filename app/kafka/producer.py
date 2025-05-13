import json

from aiokafka import AIOKafkaProducer

from app.config import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY, SHOP_ID


producer = None


async def init_producer():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',)
    await producer.start()

async def close_producer():
    await producer.stop()

async def update_products_quantity_to_oltp(updated_products: dict[int, int]):
    topic = KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY
    payload = {
        'shop_id': SHOP_ID,
        'updated_products': updated_products
        }
    key = str(SHOP_ID).encode()
    print(f"Sending products quantity: {payload} with key: {key}")
    await producer.send_and_wait(topic, json.dumps(payload).encode(), key=key)