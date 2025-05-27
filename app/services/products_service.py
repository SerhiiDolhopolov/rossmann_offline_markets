import random
import datetime
import logging

from sqlalchemy.orm import Session

from app import Shop, Employee
from app.models import Product, Category, Transaction, TransactionItem
from app.employee import Employee
from app.clickhouse.delivery_report import DeliveryReport, DeliveryReportData
from app.clickhouse import get_client

logger = logging.getLogger(__name__)


def select_products(db: Session):
    available_products = (
        db.query(Product)
        .filter(
            Product.stock_quantity > 0,
            Product.is_deleted == False,
        )
        .all()
    )
    if not available_products:
        logger.debug("No products available for selection.")
        return

    possible_count = [
        i for i in range(1, min(8, len(available_products)) + 1)
    ]
    weights = [
        len(possible_count) - i * 0.8 for i in possible_count
    ]
    items_count = random.choices(possible_count, weights=weights, k=1)[0]
    selected_products = dict()

    for _ in range(items_count):
        product = random.choice(available_products)
        available_products.remove(product)
        possible_quantity = [
            i for i in range(1, min(5, product.stock_quantity) + 1)
        ]
        weights = [len(possible_quantity) - i * 0.8 for i in possible_quantity]
        quanity = random.choices(possible_quantity, weights=weights, k=1)[0]
        selected_products[product] = quanity
    return selected_products


def pay_for_products(
    db: Session, 
    terminal_id: int, 
    cashier: Employee, 
    updated_products_id: set[int],
    factor: float = 1.0,
):
    products = select_products(db)
    if not products:
        logger.debug("No products selected for transaction.")
        return
    else:
        logger.debug(
        "Selected products: %s",
        ", ".join(f"{prod.name} (x{qty})" for prod, qty in products.items())
    )

    transaction_items = []
    transaction_amount = 0

    for product, quantity in products.items():
        product_price = product.price * factor
        unit_price = product_price * (1 - product.discount)
        unit_price = round(unit_price, 2)
        transaction_item = TransactionItem(
            product=product,
            quantity=quantity,
            unit_price=unit_price,
        )
        transaction_items.append(transaction_item)
        transaction_amount += unit_price * quantity
        product.stock_quantity -= quantity
        updated_products_id.add(product.product_id)

    total_amount = transaction_amount
    payment_method = random.choice(["Cash", "Credit Card", "Debit Card"])
    discount_type = "percentage"
    if discount_type == "percentage":
        loyalty_discount = random.choice([0, 0, 0, 0, 0, 0.05, 0.1, 0.2, 0.3])
        total_amount = total_amount * (1 - loyalty_discount)
    elif discount_type == "fixed":
        total_amount = total_amount - loyalty_discount

    total_amount = round(total_amount, 2)
    transaction_amount = round(transaction_amount, 2)
    transaсtion = Transaction(
        terminal_id=terminal_id,
        cashier_id=cashier.employee_id,
        transaction_time=datetime.datetime.now(),
        amount=transaction_amount,
        loyalty_discount=loyalty_discount,
        total_amount=total_amount,
        discount_type=discount_type,
        payment_method=payment_method,
    )
    transaсtion.transaction_items = transaction_items
    db.add(transaсtion)
    db.commit()
    db.refresh(transaсtion)
    logger.debug(
        "Transaction completed. Transaction ID: %s, Total amount: %.2f",
        transaсtion.transaction_id,
        transaсtion.amount,
    )


async def do_delivery(
    db: Session, 
    shop: Shop, 
    admin: Employee, 
    courier: Employee, 
    updated_products_id: set[int],
):
    delivery_products = (
        db.query(Product, Category)
        .join(Category, Product.category_id == Category.category_id)
        .filter(Product.is_deleted == False)
        .all()
    )

    if not delivery_products:
        logger.debug("No products available for delivery.")
        return

    delivery_products = random.sample(
        delivery_products,
        max(1, random.randint(1, len(delivery_products))),
    )
    
    delivery_report = DeliveryReport()
    for product, category in delivery_products:
        delivery_quantity = random.randrange(200, 500, 10)
        product.stock_quantity += delivery_quantity
        db.add(product)
        updated_products_id.add(product.product_id)
        
        report_data = DeliveryReportData(
            shop_id=shop.shop_id,
            country=shop.country_name,
            city=shop.city_name,
            admin_id=admin.employee_id,
            courier_id=courier.employee_id,
            product_id=product.product_id,
            product_name=product.name,
            category_id=category.category_id,
            category_name=category.name,
            quantity=delivery_quantity,
            accepted_time=datetime.datetime.now(),
        )
        delivery_report.add_report_data(report_data)
        logger.debug(
            "Product %s (ID: %d) delivered. "
            "Quantity: %d, Category: %s",
            product.name,
            product.product_id,
            delivery_quantity,
            category.name,
        )
    db.commit()

    async with get_client() as client:
        await delivery_report.save_report(client)


def get_quantity_of_updated_products(
    db: Session, 
    updated_products_id: set[int],
) -> dict[int, int]:
    """
    Returns a dictionary of updated products with their IDs and quantities.
    """
    
    if updated_products_id is None:
        updated_products_id = set()

    updated_products = (
        db.query(Product)
        .filter(Product.product_id.in_(updated_products_id))
        .all()
    )
    updated_products = {
        item.product_id: item.stock_quantity for item in updated_products
    }
    updated_products_id.clear()
    return updated_products
