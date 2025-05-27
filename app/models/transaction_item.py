from sqlalchemy import ForeignKey
from sqlalchemy import Integer, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class TransactionItem(Base):
    __tablename__ = "transaction_items"

    item_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
    )

    transaction_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("transactions.transaction_id", ondelete="CASCADE"),
        nullable=False,
    )
    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.product_id", ondelete="RESTRICT"),
        nullable=False,
    )

    quantity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    unit_price: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    transaction = relationship(
        "Transaction",
        back_populates="transaction_items",
    )
    product = relationship(
        "Product",
        back_populates="transaction_items",
    )
