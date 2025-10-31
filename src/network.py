import asyncio
import logging
from http import HTTPStatus
from ssl import SSLContext
from typing import NamedTuple

from aiohttp import ClientError, ClientTimeout, ClientSession

from tenacity import retry, wait_fixed, retry_if_exception_type, stop_after_attempt

from src.configurations import ApplicationConfiguration
from src.models.exceptions import RetryableHTTPStatusException


class HTTPClient:

    @retry(
        stop=stop_after_attempt(ApplicationConfiguration.REQUEST_RETRY_COUNT),
        wait=wait_fixed(ApplicationConfiguration.REQUESTS_RETRY_DELAY_SECONDS),
        retry=retry_if_exception_type((asyncio.TimeoutError, ClientError)),
        reraise=True
    )
    async def fetch_page(self, session: ClientSession, ssl_context: SSLContext, url: str) -> tuple[int, str, str]:
        async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
            status: int = response.status
            text: str = await response.text()
            if status != HTTPStatus.OK:
                logging.error(
                    f"Error fetching {url}: HTTP {status}"
                )
                raise RetryableHTTPStatusException(url, status)
            logging.info(f"Fetched page successfully: {url}")
            return status, text, url

    async def fetch_page_limited(self, session: ClientSession, ssl_context: SSLContext, url: str, semaphore: asyncio.Semaphore) -> tuple[
        int, str, str]:

        async with semaphore:
            return await self.fetch_page(session, ssl_context, url)
