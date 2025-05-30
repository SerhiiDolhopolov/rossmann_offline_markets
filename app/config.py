import os
import json
import logging
import logging.config

from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

OLTP_API_HOST = os.getenv("OLTP_API_HOST")
OLTP_API_PORT = os.getenv("OLTP_API_PORT")

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY = os.getenv(
    "KAFKA_TOPIC_OLTP_UPDATE_PRODUCT_QUANTITY"
)
KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY = os.getenv(
    "KAFKA_TOPIC_LOCAL_DB_UPSERT_CATEGORY"
)
KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT = os.getenv(
    "KAFKA_TOPIC_LOCAL_DB_UPSERT_PRODUCT"
)
KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC = os.getenv(
    "KAFKA_TOPIC_LOCAL_DB_UPDATE_PRODUCT_DESC"
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SHOP_REPORTS_DB = os.getenv("CLICKHOUSE_SHOP_REPORTS_DB")

SHOP_ID = os.getenv("SHOP_ID")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
COURIER_EMAIL = os.getenv("COURIER_EMAIL")
CASHIER_1_EMAIL = os.getenv("CASHIER_1_EMAIL")
CASHIER_2_EMAIL = os.getenv("CASHIER_2_EMAIL")
CASHIER_3_EMAIL = os.getenv("CASHIER_3_EMAIL")

ENV = os.getenv("ENV", "development").lower()
LOGGING_CONFIG = (
    "app/logging_config.prod.json"
    if ENV == "production"
    else "app/logging_config.dev.json"
)


def init_logging():
    with open(LOGGING_CONFIG, "r") as f:
        config = json.load(f)
    for handler in config.get("handlers", {}).values():
        filename = handler.get("filename")
        if filename:
            filename = filename.format(SHOP_ID=SHOP_ID)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            handler["filename"] = filename
    logging.config.dictConfig(config)
