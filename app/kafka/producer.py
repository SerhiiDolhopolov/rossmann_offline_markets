import json
import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import (
    KAFKA_HOST,
    KAFKA_PORT,
    KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY,
    SHOP_ID,
)

logger = logging.getLogger(__name__)


async def init_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
    )
    await producer.start()
    return producer


async def close_producer(producer: AIOKafkaProducer):
    await producer.stop()


async def send_with_reconnect(
    producer: AIOKafkaProducer,
    topic, 
    payload, 
    headers=None, 
    key=None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> AIOKafkaProducer:
    if not headers:
        headers = []
    attempt = 0
    while attempt < max_retries:
        try:
            await producer.send_and_wait(
                topic,
                payload,
                headers=headers,
                key=key,
            )
            return producer
        except KafkaConnectionError as e:
            logger.warning(
                "Kafka connection error: %s, reconnecting... (attempt %s)",
                e,
                attempt + 1,
            )
            await close_producer(producer)
            producer = await init_producer()
        except Exception as e:
            logger.error(
                "Unexpected error while sending to Kafka: %s (attempt %s)",
                e,
                attempt + 1,
                exc_info=True,
            )
            if attempt == max_retries - 1:
                raise
        attempt += 1
        await asyncio.sleep(retry_delay)
    return producer


async def update_products_quantity_to_oltp(
    producer: AIOKafkaProducer,
    updated_products: dict[int, int]
) -> AIOKafkaProducer:
    topic = KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY
    payload = {
        "shop_id": SHOP_ID,
        "updated_products": updated_products,
    }
    key = str(SHOP_ID).encode()
    logger.debug(
        "Sending products quantity with key: %s to OLTP: %s", 
        key,
        payload
    )
    producer = await send_with_reconnect(
        producer,
        topic,
        json.dumps(payload).encode(),
        headers=None,
        key=key,
    )
    return producer
