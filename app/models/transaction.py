from datetime import datetime
from decimal import Decimal

from sqlalchemy import String, Numeric
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Transaction(Base):
    __tablename__ = "transactions"
    
    transaction_id: Mapped[int] = mapped_column(primary_key=True)
    terminal_id: Mapped[int]
    cashier_id: Mapped[int]
    transaction_time: Mapped[datetime]
    amount: Mapped[float]
    loyalty_discount: Mapped[Decimal] = mapped_column(default=0.0)
    discount_type: Mapped[str] = mapped_column(String(50), default="percentage")
    total_amount: Mapped[float]
    payment_method: Mapped[str] = mapped_column(String(50))
    
    transaction_items = relationship("TransactionItem", back_populates="transaction")