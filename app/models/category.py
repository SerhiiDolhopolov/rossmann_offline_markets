from sqlalchemy import Integer, String, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Category(Base):
    __tablename__ = "categories"

    category_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=False,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )

    products = relationship(
        "Product",
        back_populates="category",
    )
