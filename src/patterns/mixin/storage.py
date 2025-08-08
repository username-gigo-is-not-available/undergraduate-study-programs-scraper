from pathlib import Path
from typing import NamedTuple
from miniopy_async import Minio

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.clients import MinioClient
from src.patterns.mixin.validation import SchemaValidationMixin
from src.patterns.strategy.storage import LocalStorage, MinioStorage


class StorageMixin:

    @classmethod
    async def get_storage_strategy(cls) -> type[LocalStorage | MinioStorage]:
        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            if not StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH.exists():
                StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH.mkdir(parents=True)
            return LocalStorage
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            minio_client: Minio = MinioClient.connect()
            bucket_exists: bool = await minio_client.bucket_exists(StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME)
            if not bucket_exists:
                await minio_client.make_bucket(StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME)
            return MinioStorage
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    @classmethod
    async def load_schema(cls, configuration: DatasetConfiguration) -> dict:
        storage_strategy: type[LocalStorage | MinioStorage] = await cls.get_storage_strategy()
        return await storage_strategy.load_schema(configuration.schema_configuration.file_name)


    @classmethod
    async def save_data(cls, data: list[NamedTuple], configuration: DatasetConfiguration) -> list[NamedTuple]:
        storage_strategy: type[LocalStorage | MinioStorage] = await cls.get_storage_strategy()
        schema: dict = await cls.load_schema(configuration)
        return await storage_strategy.save_data(data, configuration.output_io_configuration.file_name,schema)
