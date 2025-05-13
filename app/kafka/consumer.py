from typing import Awaitable
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from app.config import KAFKA_HOST, KAFKA_PORT, SHOP_ID


async def consume_messages(topic_handlers: dict[str, Awaitable]):
    consumer = AIOKafkaConsumer(
        *topic_handlers.keys(),
        bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',
        group_id=f'rossmann-local-shop-{SHOP_ID}',
        auto_offset_reset='latest',
    )
    await consumer.start()
    
    try:
        print(f"Listening to topics: {list(topic_handlers.keys())} ...")
        async for msg in consumer:
            topic = msg.topic
            value = msg.value.decode()
            print(f"Received message from topic '{topic}': {value}")
            headers = {k: v.decode() if v else None for k, v in (msg.headers or [])}
            updated_at_utc = headers.get('updated_at_utc')
            if not updated_at_utc:
                print("No 'updated_at_utc' header found in the message.")
                continue
            try:
                updated_at_utc = datetime.fromisoformat(updated_at_utc.replace('Z', '+00:00'))
            except ValueError:
                print(f"Invalid 'updated_at_utc' format: {updated_at_utc}")
                continue
            if topic in topic_handlers:
                try:
                    await topic_handlers[topic](value, updated_at_utc)
                except Exception as e:
                    print(f"Error in handler for topic '{topic}': {e}")
    finally:
        await consumer.stop()
