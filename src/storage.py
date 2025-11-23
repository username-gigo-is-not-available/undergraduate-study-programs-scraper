import logging
from dataclasses import asdict
from typing import Any

import pyarrow as pa
from miniopy_async import Minio
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from src.configurations import StorageConfiguration, TableConfiguration
from src.models.enums import FileIOType
from src.models.types import Record


class IcebergClient:
    _instance: 'IcebergClient' = None
    _s3_client: Minio = None
    _catalog: Catalog = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._catalog is not None:
            return
        logging.info("Initializing IcebergClient resources...")
        self._catalog = load_catalog(
            StorageConfiguration.ICEBERG_CATALOG_NAME
        )
        if self._s3_client is None and StorageConfiguration.FILE_IO_TYPE == FileIOType.S3:
            self._s3_client = Minio(
                endpoint=StorageConfiguration.S3_ENDPOINT_URL,
                access_key=StorageConfiguration.S3_ACCESS_KEY,
                secret_key=StorageConfiguration.S3_SECRET_KEY,
                secure=False
            )

    def get_catalog(self) -> Catalog:
        return self._catalog

    def get_s3_client(self) -> Minio | None:
        return self._s3_client

    @classmethod
    def get_table_identifier(cls,  namespace: str, table_name: str) -> str:
        return f"{namespace}.{table_name}"

    @classmethod
    def to_arrow(cls, data: list[Record], schema: Schema) -> pa.Table:
        return pa.Table.from_pylist(mapping=([asdict(row) for row in data]), schema=schema.as_arrow())

    async def save_data(self, data: list[Record], table_configuration: TableConfiguration) -> list[dict[str, Any]]:
        catalog: Catalog = self.get_catalog()

        table_identifier: str = self.get_table_identifier(StorageConfiguration.ICEBERG_NAMESPACE, table_configuration.dataset_name)
        table: Table = catalog.load_table(table_identifier)
        logging.info(f"Saving data to {table_identifier} with schema {table_configuration.dataset_name} and {len(data)} rows")

        with table.transaction() as transaction:
            transaction.append(self.to_arrow(data, table_configuration.schema))

        logging.info(f"Created snapshot_id: {table.current_snapshot().snapshot_id} for table {table_identifier}")
        return data


