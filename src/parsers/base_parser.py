import re

from bs4 import Tag

from src.config import Config
from src.patterns.mixin.data_processing import ProcessingMixin
from src.patterns.mixin.file_storage import FileStorageMixin
from src.patterns.mixin.http_client import HTTPClientMixin
from src.patterns.mixin.thread_safety import ThreadSafetyMixin


class Parser(ProcessingMixin, FileStorageMixin, HTTPClientMixin, ThreadSafetyMixin):

    @classmethod
    def extract_text(cls, tag: Tag, selector: str) -> str:
        return tag.select_one(selector).text.strip()

    @classmethod
    def extract_url(cls, tag: Tag, selector: str) -> str:
        return ''.join([Config.BASE_URL, tag.select_one(selector)['href']])

