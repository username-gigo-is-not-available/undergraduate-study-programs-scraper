import csv
import logging
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, NamedTuple

import pandas as pd
from minio import S3Error

from src.config import Config


class StorageStrategy:
    async def save_data(self, data: pd.DataFrame, output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        raise NotImplementedError


class LocalStorage(StorageStrategy):

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


class MinioStorage(StorageStrategy):
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

            Config.MINIO_CLIENT.put_object(
                bucket_name=Config.MINIO_BUCKET_NAME,
                object_name=str(output_file_name),
                data=bytes_buffer,
                length=data_length,
                content_type='text/csv'
            )
            return data
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {Config.MINIO_BUCKET_NAME}: {e}")
