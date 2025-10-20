import logging
from typing import NamedTuple, Any

from pyiceberg.catalog import load_catalog, Catalog
from miniopy_async import Minio
from pyiceberg.table import Table

from src.configurations import StorageConfiguration, DatasetConfiguration
import pyarrow as pa

from src.models.enums import FileIOType


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

    async def save_data(self, data: list[NamedTuple], dataset_configuration: DatasetConfiguration) -> list[dict[str, Any]]:
        catalog: Catalog = self.get_catalog()

        data: list[dict[str, Any]] = [row._asdict() for row in data]
        table_identifier: str = self.get_table_identifier(StorageConfiguration.ICEBERG_NAMESPACE, dataset_configuration.dataset_name)
        table: Table = catalog.load_table(table_identifier)

        logging.info(f"Saving data to {table_identifier} with schema {dataset_configuration.schema} and {len(data)} rows")

        table.append(pa.Table.from_pylist(mapping=data, schema=dataset_configuration.schema.as_arrow()))

        logging.info(f"Created snapshot_id: {table.current_snapshot().snapshot_id} for table {table_identifier}")
        return data


