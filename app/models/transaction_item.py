from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class TransactionItem(Base):
    __tablename__ = "transaction_items"
    
    item_id: Mapped[int] = mapped_column(primary_key=True)
    transaction_id: Mapped[int] = mapped_column(Integer, ForeignKey("transactions.transaction_id"))
    product_id: Mapped[int] = mapped_column(Integer, ForeignKey("products.product_id"))
    quantity: Mapped[int]
    unit_price: Mapped[float]
    
    transaction = relationship("Transaction", back_populates="transaction_items")
    product = relationship("Product", back_populates="transaction_items")