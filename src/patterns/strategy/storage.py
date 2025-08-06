import logging
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

from fastavro import writer
from miniopy_async import Minio, S3Error

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class StorageStrategy:
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, schema: dict) -> list[NamedTuple]:
        raise NotImplementedError

    @classmethod
    async def load_schema(cls, schema_path: Path) -> dict:
        raise NotImplementedError

    @classmethod
    async def serialize(cls, data: list[NamedTuple], schema: dict) -> BytesIO:
        buffer = BytesIO()
        writer(buffer, schema, [record._asdict() for record in data])
        buffer.seek(0)
        return buffer


class LocalStorage(StorageStrategy):

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, schema: dict) -> list[NamedTuple]:
        path = StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH / output_file_name
        try:
            logging.info(f"Saving data to local storage {path}")
            buffer: BytesIO = await cls.serialize(data, schema)
            with open(path, "wb") as f:
                f.write(buffer.read())
            return data
        except OSError as e:
            logging.error(f"Failed to save data to file {path}: {e}")
            return []


class MinioStorage(StorageStrategy):


    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, schema: dict) -> list[NamedTuple]:
        try:
            buffer: BytesIO = await cls.serialize(data, schema)
            data_length = buffer.getbuffer().nbytes

            logging.info(f"Saving data to MinIO bucket {StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME} as {output_file_name}")
            minio: Minio = MinioClient.connect()
            await minio.put_object(
                bucket_name=StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME,
                object_name=str(output_file_name),
                data=buffer,
                length=data_length,
                        content_type='application/avro'
            )
            return data
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME}: {e}")
            return []
