import logging
from pathlib import Path
from miniopy_async import Minio
import src.setup
from pyiceberg.schema import Schema
from pyiceberg.catalog import Catalog

from src.configurations import StorageConfiguration, STUDY_PROGRAMS_DATASET_CONFIGURATION, \
    CURRICULA_DATASET_CONFIGURATION, COURSES_DATASET_CONFIGURATION
from src.models.enums import FileIOType
from src.storage import IcebergClient


async def create_warehouse_if_not_exists() -> None:
    if StorageConfiguration.FILE_IO_TYPE == FileIOType.LOCAL:
        folder: Path = StorageConfiguration.LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH
        if folder.exists():
            logging.info(f"Path {folder} already exists")
            return
        logging.info(f"Creating folder '{folder}'")
        folder.mkdir(parents=True, exist_ok=True)
    elif StorageConfiguration.FILE_IO_TYPE == FileIOType.S3:
        bucket_name: str = StorageConfiguration.S3_ICEBERG_LAKEHOUSE_BUCKET_NAME
        s3_client: Minio = IcebergClient().get_s3_client()
        if await s3_client.bucket_exists(bucket_name):
            logging.info(f"Bucket {bucket_name} already exists")
            return
        logging.info(f"Creating bucket '{bucket_name}'")
        await s3_client.make_bucket(bucket_name)

async def initialize():
    logging.info("Initializing...")
    await create_warehouse_if_not_exists()
    iceberg_client: IcebergClient() = IcebergClient()
    catalog: Catalog = iceberg_client.get_catalog()

    namespace: str = StorageConfiguration.ICEBERG_NAMESPACE

    logging.info(f"Creating namespace '{namespace}'")
    catalog.create_namespace_if_not_exists(namespace)

    datasets = [
        STUDY_PROGRAMS_DATASET_CONFIGURATION,
        CURRICULA_DATASET_CONFIGURATION,
        COURSES_DATASET_CONFIGURATION
    ]

    for dataset in datasets:
        table_identifier: str = iceberg_client.get_table_identifier(namespace, dataset.dataset_name)
        schema: Schema = dataset.schema
        logging.info(f"Creating table '{table_identifier}'")
        catalog.create_table_if_not_exists(table_identifier, schema)
    logging.info("Initialization complete!")


