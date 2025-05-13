import csv
from sqlalchemy.orm import Session
from pathlib import Path
import datetime

from app.models import Transaction, TransactionItem, Product, Category
from app import Shop, Employee


def make_transactions_report(db: Session, shop: Shop, admin: Employee):
    report_data = []
    transactions_info = db.query(Transaction, TransactionItem, Product, Category) \
            .join(TransactionItem, Transaction.transaction_id == TransactionItem.transaction_id) \
            .join(Product, TransactionItem.product_id == Product.product_id) \
            .join(Category, Product.category_id == Category.category_id) \
            .all()
            
    
    if not transactions_info:
        print("No transactions for report.")
        return
    
    for transaction, transaction_item, product, category in transactions_info:
        report_data.append({
            'shop_id': shop.shop_id,
            'country': shop.country_name,
            'city': shop.city_name,
            'terminal_id': transaction.terminal_id,
            'admin_id': admin.employee_id,
            'cashier_id': transaction.cashier_id,
            'transaction_id': transaction.transaction_id,
            'transcation_time': transaction.transaction_time,
            'payment_method': transaction.payment_method,
            'product_id': product.product_id,
            'product_barcode': product.barcode,
            'product_name': product.name,
            'category_id': product.category_id,
            'category_name': category.name,
            'product_price': product.price,
            'product_discount': product.discount,
            'unit_price': transaction_item.unit_price,
            'quantity': transaction_item.quantity,
            'transaction_amount': transaction.amount,
            'loyalty_discount': transaction.loyalty_discount,
            'discount_type': transaction.discount_type,
            'transaction_total_amount': transaction.total_amount,
        })
    report_data.sort(key=lambda x: (x['transaction_id'], x['product_id']))
    fieldnames = report_data[0].keys()
    path = Path('transactions')
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    date = datetime.datetime.now().date()
    filename = path / f'transactions_report_shop_{shop.shop_id}_{date}.csv'
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in report_data:
            writer.writerow(row)
        
    db.query(TransactionItem).delete()
    db.query(Transaction).delete()
    db.commit()