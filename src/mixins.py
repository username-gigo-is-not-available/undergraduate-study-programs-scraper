import asyncio
import contextlib
import logging
import ssl
import threading
from concurrent.futures import Executor
from pathlib import Path
from typing import NamedTuple

import aiohttp
import certifi
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup

from src.patterns.strategy import LocalStorage, MinioStorage
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

    @classmethod
    def get_storage_strategy(cls):
        if cls.STORAGE_TYPE == 'LOCAL':
            if not LocalStorage.OUTPUT_DIRECTORY_PATH.exists():
                LocalStorage.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            return LocalStorage()
        elif cls.STORAGE_TYPE == 'MINIO':
            if not MinioStorage.MINIO_CLIENT.bucket_exists(MinioStorage.MINIO_BUCKET_NAME):
                MinioStorage.MINIO_CLIENT.make_bucket(MinioStorage.MINIO_BUCKET_NAME)
            return MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {cls.STORAGE_TYPE}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        return await cls.get_storage_strategy().save_data(data, output_file_name, column_order)


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
