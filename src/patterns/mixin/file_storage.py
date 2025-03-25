from pathlib import Path
from typing import NamedTuple

from minio import Minio

from src.config import Config, MinioClient
from src.patterns.strategy.file_storage import LocalFileStorage, MinioFileStorage


class FileStorageMixin:

    @classmethod
    def get_file_storage_strategy(cls):
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            if not Config.OUTPUT_DIRECTORY_PATH.exists():
                Config.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            return LocalFileStorage()
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            minio_client: Minio = MinioClient.get_minio_client()
            if not minio_client.bucket_exists(Config.MINIO_BUCKET_NAME):
                minio_client.make_bucket(Config.MINIO_BUCKET_NAME)
            return MinioFileStorage()
        else:
            raise ValueError(f"Unsupported storage type: {Config.FILE_STORAGE_TYPE}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        return await cls.get_file_storage_strategy().save_data(data, output_file_name, column_order)
