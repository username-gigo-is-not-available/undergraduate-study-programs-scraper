import asyncio
from abc import abstractmethod
from concurrent.futures import Executor
from functools import cache
from ssl import SSLContext
from typing import NamedTuple

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import ApplicationConfiguration, TableConfiguration
from src.models.types import Record
from src.network import HTTPClient
from src.storage import IcebergClient


class Parser:

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

    @classmethod
    def get_parsed_html(cls, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, 'lxml')

    @classmethod
    @cache
    def extract_text(cls, tag: Tag, selector: str) -> str:
        element: Tag | None = tag.select_one(selector)
        if not element:
            return ""
        text: str | None = element.get_text(strip=True)
        if not text:
            return ""
        return text

    @classmethod
    @cache
    def extract_multiple_texts(cls, tag: Tag, selector: str) -> list[str]:
        return [tag.get_text(strip=True) for tag in tag.select(selector)]

    @classmethod
    @cache
    def extract_url(cls, tag: Tag, selector: str, prepend_base_url: bool = True) -> str:
        element: Tag | None = tag.select_one(selector)
        if not element:
            return ""
        url: str = tag.select_one(selector)['href']
        if prepend_base_url:
            return ''.join([ApplicationConfiguration.BASE_URL, url])
        return url

    @abstractmethod
    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Record]:
        raise NotImplementedError("Subclasses must implement the run() method.")