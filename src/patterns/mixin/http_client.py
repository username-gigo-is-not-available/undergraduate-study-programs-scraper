import logging
from ssl import SSLContext

import aiohttp
from aiohttp import ClientTimeout

from src.configurations import  ApplicationConfiguration


class HTTPClientMixin:

    @classmethod
    async def fetch_page(cls, session: aiohttp.ClientSession, ssl_context: SSLContext, url: str) -> str | None:
        try:
            async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
                    logging.info(f"Fetching page {url}")
                    return await response.text()
        except aiohttp.ClientTimeout as e:
            logging.error(f"Request to {url} timed out: {e}")
        except aiohttp.ClientError as e:
            logging.error(f"Failed to fetch the page at {url}: {e}")
