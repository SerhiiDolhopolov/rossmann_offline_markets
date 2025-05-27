from dataclasses import dataclass, asdict
from datetime import datetime

from app.clickhouse import Report, ReportData

TABLE_NAME = "transactions"


@dataclass
class TransactionsReportData(ReportData):
    shop_id: int
    country: str
    city: str
    terminal_id: int
    admin_id: int
    cashier_id: int
    transaction_id: int
    transaction_time: datetime
    payment_method: str
    product_id: int
    product_name: str
    product_barcode: str
    category_id: int
    category_name: str
    product_price: float
    product_discount: float
    unit_price: float
    quantity: int
    transaction_amount: float
    loyalty_discount: float
    discount_type: str
    transaction_total_amount: float
    
    def as_tuple(self):
        data_dict = asdict(self)
        data_dict["transaction_time"] = data_dict["transaction_time"].strftime("%Y-%m-%d %H:%M:%S")
        return tuple(data_dict.values())
    
    
class TransactionsReport(Report):
    def __init__(self):
        super().__init__(TABLE_NAME)
        
    def add_report_data(self, transcations_report_data: TransactionsReportData) -> None:
        super().add_report_data(transcations_report_data)

    
