from typing import NamedTuple

from bs4 import Tag, BeautifulSoup

from src.configurations import ApplicationConfiguration
from src.patterns.mixin.storage import StorageMixin
from src.patterns.mixin.http_client import HTTPClientMixin
from src.patterns.mixin.validation import SchemaValidationMixin


class Parser(StorageMixin, HTTPClientMixin, SchemaValidationMixin):

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

