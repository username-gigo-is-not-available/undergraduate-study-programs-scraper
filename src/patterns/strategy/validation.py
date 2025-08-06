import json
import logging
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import aiohttp
from aiohttp import ClientResponse
from fastavro import validate, parse_schema
from miniopy_async import Minio, S3Error

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class SchemaValidationStrategy:

    @classmethod
    async def load_schema(cls, schema_file_name: Path):
        raise NotImplementedError


class LocalStorageSchemaValidationStrategy(SchemaValidationStrategy):

    @classmethod
    async def load_schema(cls, schema_file_name: Path) -> str | list | dict | None:
        path: Path = StorageConfiguration.SCHEMA_DIRECTORY_PATH / schema_file_name
        try:
            logging.info(f"Reading schema from local storage: {path}")
            with open(path, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
                return parse_schema(raw)
        except OSError as e:
            logging.error(f"Failed to read schema from local storage: {path} {e}")
            return {}

class MinioStorageSchemaValidationStrategy(SchemaValidationStrategy):

    @classmethod
    async def load_schema(cls, schema_file_name: Path) -> dict:
        object_name: str = "/".join([StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME, str(schema_file_name)])
        try:
            logging.info(f"Reading schema from MinIO bucket: {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}")
            async with aiohttp.ClientSession() as session:
                minio: Minio = MinioClient.connect()
                response: ClientResponse = await minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME,
                    object_name=object_name,
                    session=session,
                )
                data: bytes = await response.read()
                buffer: BytesIO = BytesIO(data)
            return parse_schema(json.load(buffer))
        except S3Error as e:
            logging.error(
                f"Failed to read schema from MinIO bucket {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}: {e}")
            return {}
