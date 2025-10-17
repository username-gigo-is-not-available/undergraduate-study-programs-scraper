import asyncio
import logging
from ssl import SSLContext
from typing import NamedTuple

from aiohttp import ClientError, ClientTimeout, ClientSession

from tenacity import retry, wait_fixed, retry_if_exception_type, stop_after_attempt

from src.configurations import ApplicationConfiguration


class HTTPClient:

    @retry(
        stop=stop_after_attempt(ApplicationConfiguration.REQUEST_RETRY_COUNT),
        wait=wait_fixed(ApplicationConfiguration.REQUESTS_RETRY_DELAY_SECONDS),
        retry=retry_if_exception_type((asyncio.TimeoutError, ClientError)),
        reraise=True
    )
    async def fetch_page(self, session: ClientSession, ssl_context: SSLContext, url: str) -> tuple[int, str]:
        async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
                logging.info(f"Fetching page {url}")
                return response.status, await response.text()

    async def fetch_page_wrapper(self, session: ClientSession, ssl_context: SSLContext, url: str,
                                 named_tuple: NamedTuple) -> tuple[int, str, NamedTuple]:
        http_status, page_content = await self.fetch_page(session, ssl_context, url)
        return http_status, page_content, named_tuple
