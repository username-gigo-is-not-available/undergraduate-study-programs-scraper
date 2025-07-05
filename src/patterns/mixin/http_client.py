import logging
import ssl

import aiohttp
import certifi
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup

from src.configurations import StorageConfiguration, ApplicationConfiguration


class HTTPClientMixin:

    @classmethod
    async def fetch_page(cls, url: str) -> str:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
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
