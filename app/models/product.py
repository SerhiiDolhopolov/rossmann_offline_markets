from sqlalchemy import ForeignKey
from sqlalchemy import Integer, Float, String, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=False,
    )

    category_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("categories.category_id", ondelete="RESTRICT"),
        nullable=False,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
    )
    barcode: Mapped[str] = mapped_column(
        String(12),
        unique=True,
        nullable=False,
    )
    price: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    discount: Mapped[float] = mapped_column(
        Float,
        default=0.0,
        nullable=False,
    )
    stock_quantity: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )

    category = relationship(
        "Category",
        back_populates="products",
    )
    transaction_items = relationship(
        "TransactionItem",
        back_populates="product",
    )
