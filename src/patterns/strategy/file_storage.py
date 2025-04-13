import asyncio
import csv
import logging
from io import StringIO, BytesIO
from pathlib import Path
from typing import NamedTuple, BinaryIO

from minio import S3Error, Minio

from src.config import Config, MinioClient


class FileStorageStrategy:
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        raise NotImplementedError


class LocalFileStorage(FileStorageStrategy):

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:
            with open(f"{Path(Config.OUTPUT_DIRECTORY_PATH / output_file_name)}", "w", newline="", encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(column_order)
                writer.writerows(data)
            return data
        except OSError as e:
            logging.error(f"Failed to save data to file {output_file_name}: {e}")


class MinioFileStorage(FileStorageStrategy):
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:

            string_buffer: StringIO = StringIO()
            writer = csv.writer(string_buffer)
            writer.writerow(column_order)
            writer.writerows(data)

            bytes_buffer: BinaryIO = BytesIO()
            bytes_buffer.write(string_buffer.getvalue().encode('utf-8'))
            data_length = bytes_buffer.tell()
            bytes_buffer.seek(0)

            logging.info(f"Saving data to MinIO bucket {Config.MINIO_BUCKET_NAME} as {output_file_name}")

            minio_client: Minio = MinioClient.connect()
            await asyncio.to_thread(minio_client.put_object,
                              bucket_name=Config.MINIO_BUCKET_NAME,
                              object_name=str(output_file_name),
                              data=bytes_buffer,
                              length=data_length,
                              content_type='text/csv',
                              ) # noqa
            return data
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {Config.MINIO_BUCKET_NAME}: {e}")
