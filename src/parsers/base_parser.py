from abc import abstractmethod
from concurrent.futures import Executor
from ssl import SSLContext
from typing import NamedTuple

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import ApplicationConfiguration, IcebergTableConfiguration
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
    def extract_text(cls, tag: Tag, selector: str) -> str:
        return tag.select_one(selector).text.strip()

    @classmethod
    def extract_url(cls, tag: Tag, selector: str) -> str:
        return ''.join([ApplicationConfiguration.BASE_URL, tag.select_one(selector)['href']])

    @abstractmethod
    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: IcebergTableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  executor: Executor | None = None) -> list[NamedTuple]:
        raise NotImplementedError("Subclasses must implement the run() method.")