from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import Category
from app.models import Product


def sync_category(
    db: Session,
    is_deleted: bool,
    category_id: int,
    name: str,
    description: str = None,
):
    stmt = (
        insert(Category)
        .values(
            category_id=category_id,
            name=name,
            description=description,
            is_deleted=is_deleted,
        )
        .on_conflict_do_update(
            index_elements=["category_id"],
            set_={
                "category_id": category_id,
                "name": name,
                "description": description,
                "is_deleted": is_deleted,
            },
        )
    )
    db.execute(stmt)
    db.commit()


def sync_product(
    db: Session,
    is_deleted: bool,
    product_id: int,
    name: str,
    description: str = None,
    barcode: str = None,
    category_id: int = None,
    price: float = 0.0,
    discount: float = 0.0,
):
    stmt = (
        insert(Product)
        .values(
            product_id=product_id,
            name=name,
            description=description,
            barcode=barcode,
            category_id=category_id,
            price=price,
            discount=discount,
            stock_quantity=0,
            is_deleted=is_deleted,
        )
        .on_conflict_do_update(
            index_elements=["product_id"],
            set_={
                "product_id": product_id,
                "name": name,
                "description": description,
                "barcode": barcode,
                "category_id": category_id,
                "price": price,
                "discount": discount,
                "is_deleted": is_deleted,
            },
        )
    )
    db.execute(stmt)
    db.commit()


def update_product_desc(
    db: Session,
    is_deleted: bool,
    product_id: int,
    name: str,
    description: str = None,
    barcode: str = None,
):
    product = db.query(Product).filter(
        Product.product_id == product_id).first()
    if product:
        product.name = name
        product.description = description
        product.barcode = barcode
        product.is_deleted = is_deleted
        db.commit()
    else:
        return

    db.commit()
