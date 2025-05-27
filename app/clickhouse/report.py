from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging
import asyncio

from aiochclient import ChClient

logger = logging.getLogger(__name__)


@dataclass
class ReportData(ABC):
    @abstractmethod
    def as_tuple(self):
        pass

    @classmethod
    def get_keys(cls):
        return tuple(cls.__dataclass_fields__.keys())


class Report(ABC):
    def __init__(self, table_name: str):
        self.TABLE_NAME = table_name
        self.report_data: list[ReportData] = []
    
    @abstractmethod
    def add_report_data(self, report_data: ReportData) -> None:
        self.report_data.append(report_data)

    async def save_report(self, client: ChClient, max_retries: int = 10):
        logger.info("Saving %s report to ClickHouse...", self.TABLE_NAME)
        if not self.report_data:
            logger.info("No report data to save for %s.", self.TABLE_NAME)
            return False
        keys = self.report_data[0].__class__.get_keys()
        query = f"""
            INSERT INTO {self.TABLE_NAME} (
                {", ".join(keys)}
            ) VALUES
        """
        values = [row.as_tuple() for row in self.report_data]
        for attempt in range(max_retries):
            try:
                await client.execute(query, *values)
                self.report_data = []
                return True
            except Exception as e:
                logger.error(
                    "Error saving %s report to ClickHouse. %s\nAttempt %s.", 
                    self.TABLE_NAME,
                    e,
                    attempt + 1,
                    exc_info=True,
                )
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(600)