import asyncio
import threading
from abc import abstractmethod
from functools import cache

from bs4 import Tag, BeautifulSoup

from src.models.types import Record


class BaseParser:
    BASE_URL: str = "https://finki.ukim.mk"

    @property
    @abstractmethod
    def accreditation_year(self) -> int:
        raise NotImplementedError("Subclasses must implement this property")

    @classmethod
    def set_event(cls, event: asyncio.Event | threading.Event) -> None:
        if event.is_set():
            return
        event.set()

    @classmethod
    def get_parsed_html(cls, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, 'lxml')

    @classmethod
    def decompose(cls, tag: Tag, selector: str) -> Tag:
        return tag.find(selector).extract()

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
    def extract_texts_from_node(cls, tag: Tag, selector: str) -> list[str]:
        element: Tag | None = tag.select_one(selector)
        if not element:
            return []

        return element.find_all(text=True, recursive=False)

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
            return ''.join([cls.BASE_URL, url])
        return url

    @abstractmethod
    def parse_row(self, *args, **kwargs) -> Record:
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def parse_data(self, *args, **kwargs) -> list[Record] | Record:
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def run(self, *args, **kwargs) -> list[Record] | Record:
        raise NotImplementedError("Subclasses must implement this method.")

