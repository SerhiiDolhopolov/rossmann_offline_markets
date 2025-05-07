import random
from datetime import datetime
import csv

from sqlalchemy.orm import Session

from app.models import Product, Category
from app import Shop, Employee
from app.config import DATE_TIME_FORMAT


def do_delivery(db: Session, shop: Shop, admin: Employee, courier: Employee) -> None:
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
        delivery_quantity = random.randrange(100, 200, 10)
        product.stock_quantity += delivery_quantity
        db.add(product)
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
            'accepted_time': datetime.now().strftime(DATE_TIME_FORMAT),
        })
    db.commit()
    
    order_data.sort(key=lambda x: (x['category_id'], x['product_id']))
    fieldnames = order_data[0].keys()
    file_name = f'order_shop_{shop.shop_id}_{datetime.now().strftime(DATE_TIME_FORMAT)}.csv'
    with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in order_data:
            writer.writerow(row)
