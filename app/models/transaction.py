from datetime import datetime, timezone

from sqlalchemy import Integer, Float, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
    )

    terminal_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    cashier_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    transaction_time: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.now(timezone.utc),
        nullable=False,
    )
    amount: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    loyalty_discount: Mapped[float] = mapped_column(
        Float,
        default=0.0,
        nullable=False,
    )
    discount_type: Mapped[str] = mapped_column(
        String(50),
        default="percentage",
        nullable=False,
    )
    total_amount: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    payment_method: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    transaction_items = relationship(
        "TransactionItem",
        back_populates="transaction",
    )
