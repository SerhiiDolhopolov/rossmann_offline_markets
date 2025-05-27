import logging
import asyncio
from typing import Awaitable
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config import KAFKA_HOST, KAFKA_PORT, SHOP_ID

logger = logging.getLogger(__name__)
logging.getLogger("aiokafka").setLevel(logging.INFO)


async def consume_messages(topic_handlers: dict[str, Awaitable]):
    while True:
        consumer = AIOKafkaConsumer(
            *topic_handlers.keys(),
            bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
            group_id=f"rossmann-local-shop-{SHOP_ID}",
            auto_offset_reset="latest",
        )

        try:
            await consumer.start()
            logger.info(
                "Consumer started for topics: %s",
                list(topic_handlers.keys()),
            )
            async for msg in consumer:
                topic = msg.topic
                value = msg.value.decode()
                logger.debug(
                    "Received message from topic '%s': %s",
                    topic,
                    value,
                )
                
                if topic in topic_handlers:
                    try:
                        await topic_handlers[topic](value)
                    except Exception as e:
                        logger.error(
                            "Error in handler for topic '%s': %s",
                            topic,
                            e,
                            exc_info=True,
                        )

        except KafkaConnectionError as e:
            logger.error(
                "Kafka connection error: %s. Reconnecting in 10 seconds...",
                e,
                exc_info=True,
            )
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(
                "Unexpected error: %s. Reconnecting in 10 seconds...",
                e,
                exc_info=True,
            )
            await asyncio.sleep(10)
        finally:
            if not consumer._closed:
                await consumer.stop()
