import asyncio
import contextlib
import csv
import logging
import ssl
import threading
from asyncio import AbstractEventLoop
from concurrent.futures import Executor
from pathlib import Path
from typing import NamedTuple

import aiohttp
import certifi
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup

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
    OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH'))

    @classmethod
    def save_data_to_file(cls, data: list[NamedTuple], field_names: list[str], file_name: Path) -> None:
        try:
            with open(f"{Path(cls.OUTPUT_DIRECTORY_PATH / file_name)}", "w", newline="", encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(field_names)
                writer.writerows(data)
        except OSError as e:
            logging.error(f"Failed to save data to file {file_name}: {e}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], file_name: Path, executor: Executor = None) -> None:
        loop: AbstractEventLoop = asyncio.get_event_loop()
        cls.OUTPUT_DIRECTORY_PATH.mkdir(parents=True, exist_ok=True)
        logging.info(f"Saving data to file {file_name}")
        await loop.run_in_executor(executor, cls.save_data_to_file, data, list(data)[0]._fields, file_name)


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
