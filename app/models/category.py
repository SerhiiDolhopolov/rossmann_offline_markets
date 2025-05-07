from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Category(Base):
    __tablename__ = "categories"

    category_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(String(2048), nullable=True)
    is_deleted: Mapped[bool] 
    
    products = relationship("Product", back_populates="category")