import csv
import logging
from io import StringIO, BytesIO
from pathlib import Path
from typing import NamedTuple, BinaryIO, TextIO

from miniopy_async import Minio, S3Error
from src.config import Config
from src.clients import MinioClient


class FileStorageStrategy:
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        raise NotImplementedError

    @classmethod
    def write_data(cls, data: list[NamedTuple], buffer: TextIO | StringIO, column_order: list[str]) -> list[NamedTuple]:
        writer = csv.writer(buffer)
        writer.writerow(column_order)
        writer.writerows(data)
        return data


class LocalFileStorage(FileStorageStrategy):

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:
            with open(f"{Path(Config.OUTPUT_DIRECTORY_PATH / output_file_name)}", "w", newline="", encoding='utf-8') as file:
                cls.write_data(data, file, column_order)
            return data
        except OSError as e:
            logging.error(f"Failed to save data to file {output_file_name}: {e}")
            return []


class MinioFileStorage(FileStorageStrategy):
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:

            string_buffer: StringIO = StringIO()
            cls.write_data(data, string_buffer, column_order)

            bytes_buffer: BinaryIO = BytesIO()
            bytes_buffer.write(string_buffer.getvalue().encode('utf-8'))
            data_length = bytes_buffer.tell()
            bytes_buffer.seek(0)

            logging.info(f"Saving data to MinIO bucket {Config.MINIO_BUCKET_NAME} as {output_file_name}")

            minio_client: Minio = MinioClient.connect()
            await minio_client.put_object(
                bucket_name=Config.MINIO_BUCKET_NAME,
                object_name=str(output_file_name),
                data=bytes_buffer,
                length=data_length,
                content_type='text/csv'
            )
            return data
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {Config.MINIO_BUCKET_NAME}: {e}")
            return []
