from dataclasses import dataclass, asdict
from datetime import datetime

from app.clickhouse import Report, ReportData

TABLE_NAME = "delivery"


@dataclass
class DeliveryReportData(ReportData):
    shop_id: int
    country: str
    city: str
    admin_id: int
    courier_id: int
    product_id: int
    product_name: str
    category_id: int
    category_name: str
    quantity: int
    accepted_time: datetime
    
    def as_tuple(self):
        data_dict = asdict(self)
        data_dict["accepted_time"] = data_dict["accepted_time"].strftime("%Y-%m-%d %H:%M:%S")
        return tuple(data_dict.values())
    
    
class DeliveryReport(Report):
    def __init__(self):
        super().__init__(TABLE_NAME)
        
    def add_report_data(self, delivery_report_data: DeliveryReportData) -> None:
        super().add_report_data(delivery_report_data)

    
