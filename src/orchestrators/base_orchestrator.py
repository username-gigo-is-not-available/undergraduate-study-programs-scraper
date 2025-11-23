import asyncio
from abc import ABC, abstractmethod

from src.configurations import TableConfiguration
from src.models.types import Record
from src.parsers.base_parser import BaseParser
from src.storage import IcebergClient


class BaseOrchestrator(ABC):

    def __init__(self, table_configuration: TableConfiguration, parsers: list[BaseParser]):
        self.table_configuration = table_configuration
        self.parsers = sorted(parsers, key=lambda parser: parser.accreditation_year)


    @abstractmethod
    def create_tasks(self, *args, **kwargs) -> list[asyncio.Task[Record]]:
        raise NotImplementedError("Subclasses must implement this method")

    async def run(self, iceberg_client: IcebergClient, *args, **kwargs) -> None:

        tasks: list[asyncio.Task[list[Record]]] = self.create_tasks(*args, **kwargs)
        data: list[Record] = sum(await asyncio.gather(*tasks), [])
        await iceberg_client.save_data(data, self.table_configuration)