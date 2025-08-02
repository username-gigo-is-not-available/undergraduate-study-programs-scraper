import json
import json
import logging
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import aiohttp
from aiohttp import ClientResponse
from fastavro import writer, validate
from fastavro.schema import parse_schema
from miniopy_async import Minio, S3Error

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class StorageStrategy:
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[
        NamedTuple]:
        raise NotImplementedError

    @classmethod
    async def load_schema(cls, schema_path: Path) -> dict:
        raise NotImplementedError

    @classmethod
    async def validate_records(cls, records: list[NamedTuple], schema: dict) -> bool:
        return all(validate(record._asdict(), schema) for record in records)

    @classmethod
    async def serialize(cls, data: list[NamedTuple], schema: dict) -> BytesIO:
        buffer = BytesIO()
        writer(buffer, schema, [record._asdict() for record in data])
        buffer.seek(0)
        return buffer


class LocalStorage(StorageStrategy):

    @classmethod
    async def load_schema(cls, schema_file_name: Path) -> str | list | dict | None:
        path: Path = StorageConfiguration.OUTPUT_SCHEMA_DIRECTORY_PATH / schema_file_name
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
                return parse_schema(raw)
        except OSError as e:
            logging.error(f"Failed to read schema from local storage: {StorageConfiguration.OUTPUT_SCHEMA_DIRECTORY_PATH} {e}")
            return {}

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, schema_file_name: Path) -> list[NamedTuple]:
        path = StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH / output_file_name
        try:
            schema: dict =  await cls.load_schema(schema_file_name)
            records_are_valid: bool = await cls.validate_records(data, schema)
            if not records_are_valid:
                logging.error(f"Validation failed for file {output_file_name} and schema {schema_file_name}")
                return []

            buffer: BytesIO = await cls.serialize(data, schema)
            with open(path, "wb") as f:
                f.write(buffer.read())
            return data
        except OSError as e:
            logging.error(f"Failed to save data to file {path}: {e}")
            return []


class MinioStorage(StorageStrategy):

    @classmethod
    async def load_schema(cls, schema_file_name: Path)-> dict:
        try:
            async with aiohttp.ClientSession() as session:
                minio: Minio = MinioClient.connect()
                object_name: str = "/".join([StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME, str(schema_file_name)])
                response: ClientResponse = await minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_OUTPUT_SCHEMA_BUCKET_NAME,
                    object_name=object_name,
                    session=session,
                )
                data: bytes = await response.read()
                buffer: BytesIO = BytesIO(data)
            return json.load(buffer)
        except S3Error as e:
            logging.error(
                f"Failed to read schema from MinIO bucket {StorageConfiguration.MINIO_OUTPUT_SCHEMA_BUCKET_NAME}: {e}")
            return {}

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, schema_file_name: Path) -> list[NamedTuple]:
        try:
            schema: dict = await cls.load_schema(schema_file_name)
            records_are_valid: bool = await cls.validate_records(data, schema)
            if not records_are_valid:
                logging.error(f"Validation failed for file {output_file_name} and schema {schema_file_name}")
                return []

            buffer: BytesIO = await cls.serialize(data, schema)
            data_length = buffer.getbuffer().nbytes

            logging.info(f"Saving data to MinIO bucket {StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME} as {output_file_name}")
            minio_client: Minio = MinioClient.connect()
            await minio_client.put_object(
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
