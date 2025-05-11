from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    barcode: Mapped[str] = mapped_column(String(12), unique=True)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("categories.category_id"))
    price: Mapped[float]
    discount: Mapped[float] = mapped_column(default=0.0)
    stock_quantity: Mapped[int]
    is_deleted: Mapped[bool] 
    
    category = relationship("Category", back_populates="products")
    transaction_items = relationship("TransactionItem", back_populates="product")