import random
import datetime
import csv
from pathlib import Path

from sqlalchemy.orm import Session

from app import Shop, Employee
from app.models import Product, Category, Transaction, TransactionItem
from app.employee import Employee
from app.config import DATE_TIME_FORMAT


UPDATED_PRODUCTS = set()


def select_products(db: Session):
    available_products = db.query(Product) \
                           .filter(Product.stock_quantity > 0,
                                    Product.is_deleted == False) \
                           .all()
    if not available_products:
        #print("No products available.")
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
        
        
def pay_for_products(db: Session, terminal_id: int, cashier: Employee, factor: float = 1.0):
    global UPDATED_PRODUCTS
    
    products = select_products(db)
    if not products:
        #print("No products selected.")
        return
    
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
        UPDATED_PRODUCTS.add(product.product_id)
    
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
    #print(f"Transaction completed. Transaction ID: {transaсtion.transaction_id}")
    #print(f"Total amount: {transaсtion.amount}")
    
    
def do_delivery(db: Session, shop: Shop, admin: Employee, courier: Employee) -> None:
    global UPDATED_PRODUCTS
    
    delivery_info = db.query(Product, Category) \
                      .join(Category, Product.category_id == Category.category_id) \
                      .filter(Product.is_deleted == False) \
                      .all()
    
    if not delivery_info:
        print("No products available for delivery.")
        return
    
    delivery_info = random.sample(delivery_info, max(1, random.randint(1, len(delivery_info))))
    order_data = []
    for product, category in delivery_info:
        delivery_quantity = random.randrange(200, 500, 10)
        product.stock_quantity += delivery_quantity
        db.add(product)
        UPDATED_PRODUCTS.add(product.product_id)
        print(f"Delivering product: {product.name}, Quantity: {delivery_quantity}")
        order_data.append({
            'shop_id': shop.shop_id,
            'country': shop.country_name,
            'city': shop.city_name,
            'admin_ud': admin.employee_id,
            'courier_id': courier.employee_id,
            'product_id': product.product_id,
            'product_name': product.name,
            'category_id': product.category_id,
            'category_name': category.name,
            'quantity': delivery_quantity,
            'accepted_time': datetime.datetime.now().strftime(DATE_TIME_FORMAT),
        })
    db.commit()
    
    order_data.sort(key=lambda x: (x['category_id'], x['product_id']))
    fieldnames = order_data[0].keys()
    path = Path('orders')
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    file_name = path / f'order_shop_{shop.shop_id}_{datetime.datetime.now().strftime(DATE_TIME_FORMAT)}.csv'
    with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in order_data:
            writer.writerow(row)

def get_quantity_of_updated_products(db: Session) -> dict[int, int]:
    """
        Returns a dictionary of updated products with their IDs and quantities.
    """
    global UPDATED_PRODUCTS
    
    updated_products = db.query(Product) \
                      .filter(Product.product_id.in_(UPDATED_PRODUCTS)) \
                      .all()
    updated_products = {item.product_id: item.stock_quantity for item in updated_products}
    UPDATED_PRODUCTS.clear()
    return updated_products