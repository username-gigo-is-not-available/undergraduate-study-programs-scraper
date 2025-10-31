import asyncio
import logging
from abc import abstractmethod
from concurrent.futures import Executor
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
    def extract_text(cls, tag: Tag, selector: str, recursive: bool = False) -> str:
        element: Tag | None = tag.select_one(selector)
        if not element:
            return ""
        text: str | None = element.find(text=True, recursive=recursive)
        if not text:
            return ""
        return element.find(text=True, recursive=recursive)

    @classmethod
    def extract_multiple_texts(cls, tag: Tag, selector: str, recursive: bool = False) -> list[str]:
        return [tag.find(text=True, recursive=recursive) for tag in tag.select(selector)]

    @classmethod
    def extract_url(cls, tag: Tag, selector: str) -> str:
        return ''.join([ApplicationConfiguration.BASE_URL, tag.select_one(selector)['href']])

    @abstractmethod
    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Record]:
        raise NotImplementedError("Subclasses must implement the run() method.")