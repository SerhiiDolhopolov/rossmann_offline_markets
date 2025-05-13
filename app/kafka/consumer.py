from typing import Awaitable

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
            value = msg.value.decode("utf-8")
            print(f"Received message from topic '{topic}': {value}")
            if topic in topic_handlers:
                try:
                    await topic_handlers[topic](value)
                except Exception as e:
                    print(f"Error in handler for topic '{topic}': {e}")
    finally:
        await consumer.stop()
