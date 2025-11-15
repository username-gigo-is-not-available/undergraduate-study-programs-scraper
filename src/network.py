import abc
import asyncio
import logging
from abc import abstractmethod
from http import HTTPStatus
from ssl import SSLContext
from typing import Any, Tuple

from aiohttp import ClientError, ClientTimeout, ClientSession, ClientResponse
from tenacity import retry, wait_fixed, retry_if_exception_type, stop_after_attempt

from src.configurations import ApplicationConfiguration
from src.models.exceptions import RetryableHTTPStatusException


class BaseReadingStrategy(abc.ABC):
    @abstractmethod
    async def read(self, response: ClientResponse) -> str | bytes:
        pass

class TextReadingStrategy(BaseReadingStrategy):
    async def read(self, response: ClientResponse) -> str:
        return await response.text()

class BytesReadingStrategy(BaseReadingStrategy):
    async def read(self, response: ClientResponse) -> bytes:
        return await response.read()


class HTTPClient:
    text_strategy = TextReadingStrategy()
    bytes_strategy = BytesReadingStrategy()

    @retry(
        stop=stop_after_attempt(ApplicationConfiguration.REQUEST_RETRY_COUNT),
        wait=wait_fixed(ApplicationConfiguration.REQUESTS_RETRY_DELAY_SECONDS),
        retry=retry_if_exception_type((asyncio.TimeoutError, ClientError)),
        reraise=True
    )
    async def _fetch_core(self,
                          session: ClientSession,
                          ssl_context: SSLContext,
                          url: str,
                          strategy: BaseReadingStrategy) -> Tuple[int, str | bytes, str]:

        async with session.get(url, ssl=ssl_context,
                               timeout=ClientTimeout(
                                   total=ApplicationConfiguration.REQUESTS_TIMEOUT_SECONDS)) as response:
            status: int = response.status

            data: str | bytes = await strategy.read(response)

            if status != HTTPStatus.OK:
                logging.error(f"Error fetching {url}: HTTP {status}")
                raise RetryableHTTPStatusException(url, status)

            logging.info(f"Fetched page successfully: {url}")
            return status, data, url


    async def _fetch_limited_core(self,
                                  strategy: BaseReadingStrategy,
                                  session: ClientSession,
                                  ssl_context: SSLContext,
                                  url: str,
                                  semaphore: asyncio.Semaphore,
                                  **kwargs: Any) -> Tuple:

        async with semaphore:
            status, data, url = await self._fetch_core(session, ssl_context, url, strategy)

            result = (status, data, url, *kwargs.values())
            return result if kwargs else result[:3]


    async def fetch_text(self, session: ClientSession, ssl_context: SSLContext, url: str) -> Tuple[int, str, str]:
        return await self._fetch_core(session, ssl_context, url, self.text_strategy)

    async def fetch_text_limited(self, session: ClientSession, ssl_context: SSLContext, url: str,
                                 semaphore: asyncio.Semaphore, **kwargs: Any) -> Tuple:
        return await self._fetch_limited_core(self.text_strategy, session, ssl_context, url, semaphore, **kwargs)

    async def fetch_bytes(self, session: ClientSession, ssl_context: SSLContext, url: str) -> Tuple[int, bytes, str]:
        return await self._fetch_core(session, ssl_context, url, self.bytes_strategy)

    async def fetch_bytes_limited(self, session: ClientSession, ssl_context: SSLContext, url: str,
                                  semaphore: asyncio.Semaphore, **kwargs: Any) -> Tuple:
        return await self._fetch_limited_core(self.bytes_strategy, session, ssl_context, url, semaphore, **kwargs)