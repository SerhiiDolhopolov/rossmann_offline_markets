import random
import datetime

from sqlalchemy.orm import Session

from app.models import Product, Transaction, TransactionItem
from app.employee import Employee
from app.config import DATE_TIME_FORMAT


def select_products(db: Session):
    available_products = db.query(Product) \
                           .filter(Product.stock_quantity > 0,
                                    Product.is_deleted == False) \
                           .all()
    if not available_products:
        print("No products available.")
        return
    
    possible_count = [i for i in range(1, min(8, len(available_products)) + 1)]
    weights = [len(possible_count) - i * 0.8 for i in possible_count]
    items_count = random.choices(possible_count, weights=weights, k=1)[0]
    selected_products = dict()
    
    for _ in range(items_count):
        product = random.choice(available_products)
        available_products.remove(product)
        possible_quantity = [i for i in range(1, min(5, product.stock_quantity) + 1)]
        weights = [len(possible_quantity) - i * 0.8 for i in possible_quantity]
        quanity = random.choices(possible_quantity, weights=weights, k=1)[0]
        selected_products[product] = quanity
        #print(f"Selected product: {product.name}, Quantity: {quanity}")
    return selected_products
        
        
def pay_for_products(db: Session, terminal_id: int, cashier: Employee):
    products = select_products(db)
    if not products:
        print("No products selected.")
        return
    
    transaction_items = []
    transaction_amount = 0
    
    for product, quantity in products.items():
        unit_price = product.price * (1 - product.discount)
        unit_price = round(unit_price, 2)
        transaction_item = TransactionItem(
            product=product,
            quantity=quantity,
            unit_price=unit_price,
        )
        transaction_items.append(transaction_item)
        transaction_amount += unit_price * quantity
        product.stock_quantity -= quantity
    
    total_amount = transaction_amount
    payment_method = random.choice(["Cash", "Credit Card", "Debit Card"])
    discount_type = 'percentage'
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
            transaction_time=datetime.datetime.now().strftime(DATE_TIME_FORMAT),
            amount = transaction_amount,
            loyalty_discount = loyalty_discount,
            total_amount = total_amount,
            discount_type = discount_type,
            payment_method=payment_method,
        )
    transaсtion.transaction_items = transaction_items
    db.add(transaсtion)
    db.commit()
    db.refresh(transaсtion)
    print(f"Transaction completed. Transaction ID: {transaсtion.transaction_id}")
    print(f"Total amount: {transaсtion.amount}")