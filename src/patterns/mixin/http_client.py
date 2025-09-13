import asyncio
import logging
from ssl import SSLContext
from typing import Any, Coroutine

import aiohttp
from aiohttp import ClientTimeout, ClientError
from tenacity import retry, wait_fixed, retry_if_exception_type, stop_after_attempt
from src.configurations import  ApplicationConfiguration


class HTTPClientMixin:

    @classmethod
    @retry(
        stop=stop_after_attempt(ApplicationConfiguration.REQUEST_RETRY_COUNT),
        wait=wait_fixed(ApplicationConfiguration.REQUESTS_RETRY_DELAY_SECONDS),
        retry=retry_if_exception_type((asyncio.TimeoutError, ClientError)),
        reraise=True
    )
    async def fetch_page(cls, session: aiohttp.ClientSession, ssl_context: SSLContext, url: str) -> tuple[int, str]:
        async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
                logging.info(f"Fetching page {url}")
                return response.status, await response.text()
