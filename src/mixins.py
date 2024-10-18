import asyncio
import contextlib
import csv
import logging
import ssl
import threading
from asyncio import AbstractEventLoop
from concurrent.futures import Executor
from io import StringIO, BytesIO
from pathlib import Path
from typing import NamedTuple, BinaryIO

import aiohttp
import certifi
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from minio import Minio
from minio.error import S3Error

from static import ENVIRONMENT_VARIABLES


class ThreadSafetyMixin:
    LOCK_TIMEOUT_SECONDS: int = int(ENVIRONMENT_VARIABLES.get('LOCK_TIMEOUT_SECONDS'))

    @classmethod
    @contextlib.asynccontextmanager
    async def async_lock(cls, lock: threading.Lock, executor: Executor):
        loop = asyncio.get_event_loop()
        acquired: bool = await loop.run_in_executor(executor, lock.acquire, cls.LOCK_TIMEOUT_SECONDS)
        if not acquired:
            raise TimeoutError(f"Could not acquire lock in {cls.LOCK_TIMEOUT_SECONDS} seconds")
        try:
            yield
        finally:
            lock.release()


class StorageMixin:
    STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get('STORAGE_TYPE')
    OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH'))
    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('MINIO_ENDPOINT_URL')
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_SECRET_KEY')
    MINIO_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_BUCKET_NAME')
    MINIO_CLIENT = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    @classmethod
    def save_data_to_local_storage(cls, data: list[NamedTuple], field_names: list[str], file_name: Path) -> None:
        try:
            with open(f"{Path(cls.OUTPUT_DIRECTORY_PATH / file_name)}", "w", newline="", encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(field_names)
                writer.writerows(data)
        except OSError as e:
            logging.error(f"Failed to save data to file {file_name}: {e}")

    @classmethod
    def save_data_to_minio(cls, data: list[NamedTuple], field_names: list[str], file_name: Path) -> None:

        try:

            string_buffer: StringIO = StringIO()
            writer = csv.writer(string_buffer)
            writer.writerow(field_names)
            writer.writerows(data)

            bytes_buffer: BinaryIO = BytesIO()
            bytes_buffer.write(string_buffer.getvalue().encode('utf-8'))
            data_length = bytes_buffer.tell()
            bytes_buffer.seek(0)

            logging.info(f"Saving data to MinIO bucket {cls.MINIO_BUCKET_NAME} as {file_name}")

            cls.MINIO_CLIENT.put_object(
                bucket_name=cls.MINIO_BUCKET_NAME,
                object_name=str(file_name),
                data=bytes_buffer,
                length=data_length,
                content_type='text/csv'
            )

        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {cls.MINIO_BUCKET_NAME}: {e}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], file_name: Path, executor: Executor = None) -> None:
        loop: AbstractEventLoop = asyncio.get_event_loop()

        if cls.STORAGE_TYPE == 'LOCAL':

            cls.OUTPUT_DIRECTORY_PATH.mkdir(parents=True, exist_ok=True)
            logging.info(f"Saving data to file {file_name}")
            await loop.run_in_executor(executor, cls.save_data_to_local_storage, data, list(data)[0]._fields, file_name)

        elif cls.STORAGE_TYPE == 'MINIO':

            if not cls.MINIO_CLIENT.bucket_exists(cls.MINIO_BUCKET_NAME):
                cls.MINIO_CLIENT.make_bucket(cls.MINIO_BUCKET_NAME)
                logging.info(f"Created MinIO bucket {cls.MINIO_BUCKET_NAME}")
            logging.info(f"Saving data to MinIO bucket {cls.MINIO_BUCKET_NAME}")
            await loop.run_in_executor(executor, cls.save_data_to_minio, data, list(data)[0]._fields, file_name)


class HTTPClientMixin:
    REQUESTS_TIMEOUT_SECONDS: int = int(ENVIRONMENT_VARIABLES.get('REQUESTS_TIMEOUT_SECONDS'))

    @classmethod
    async def fetch_page(cls, url: str) -> str:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=cls.REQUESTS_TIMEOUT_SECONDS)) as response:
                    logging.info(f"Fetching page {url}")
                    return await response.text()
        except aiohttp.ClientTimeout as e:
            logging.error(f"Request to {url} timed out: {e}")
        except aiohttp.ClientError as e:
            logging.error(f"Failed to fetch the page at {url}: {e}")

    @classmethod
    async def get_parsed_html(cls, url: str) -> BeautifulSoup:
        html: str = await HTTPClientMixin.fetch_page(url)
        return BeautifulSoup(html, 'lxml')
