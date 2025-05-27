import csv
import io
import datetime
import logging

from sqlalchemy.orm import Session

from app import Shop, Employee
from app.models import Transaction, TransactionItem, Product, Category
from app.clickhouse import TransactionsReport, TransactionsReportData
from app.clickhouse import get_client

logger = logging.getLogger(__name__)


async def make_transactions_report(db: Session, shop: Shop, admin: Employee):
    transactions_info = (
        db.query(Transaction, TransactionItem, Product, Category)
        .join(TransactionItem, Transaction.transaction_id == TransactionItem.transaction_id)
        .join(Product, TransactionItem.product_id == Product.product_id)
        .join(Category, Product.category_id == Category.category_id)
        .all()
    )

    if not transactions_info:
        logger.debug("No transactions found for report.")
        return
    
    transactions_report = TransactionsReport()
    for transaction, transaction_item, product, category in transactions_info:
        report_data = TransactionsReportData(
            shop_id=shop.shop_id,
            country=shop.country_name,
            city=shop.city_name,
            terminal_id=transaction.terminal_id,
            admin_id=admin.employee_id,
            cashier_id=transaction.cashier_id,
            transaction_id=transaction.transaction_id,
            transaction_time=transaction.transaction_time,
            payment_method=transaction.payment_method,
            product_id=product.product_id,
            product_barcode=product.barcode,
            product_name=product.name,
            category_id=product.category_id,
            category_name=category.name,
            product_price=product.price,
            product_discount=product.discount,
            unit_price=transaction_item.unit_price,
            quantity=transaction_item.quantity,
            transaction_amount=transaction.amount,
            loyalty_discount=transaction.loyalty_discount,
            discount_type=transaction.discount_type,
            transaction_total_amount=transaction.total_amount
        )
        transactions_report.add_report_data(report_data)
        
    async with get_client() as client:
        await transactions_report.save_report(client)

    db.query(TransactionItem).delete()
    db.query(Transaction).delete()
    db.commit()
