from pathlib import Path
from typing import NamedTuple
from miniopy_async import Minio

from src.config import Config, MinioClient
from src.patterns.strategy.file_storage import LocalFileStorage, MinioFileStorage


class FileStorageMixin:

    @classmethod
    async def get_file_storage_strategy(cls) -> type[LocalFileStorage | MinioFileStorage]:
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            if not Config.OUTPUT_DIRECTORY_PATH.exists():
                Config.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            return LocalFileStorage
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            minio_client: Minio = MinioClient.connect()
            bucket_exists: bool = await minio_client.bucket_exists(Config.MINIO_BUCKET_NAME)
            if not bucket_exists:
                await minio_client.make_bucket(Config.MINIO_BUCKET_NAME)
            return MinioFileStorage
        else:
            raise ValueError(f"Unsupported storage type: {Config.FILE_STORAGE_TYPE}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        storage_strategy: type[LocalFileStorage | MinioFileStorage] = await cls.get_file_storage_strategy()
        return await storage_strategy.save_data(data, output_file_name, column_order)
