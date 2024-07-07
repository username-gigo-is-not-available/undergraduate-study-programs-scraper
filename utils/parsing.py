import ssl

import aiohttp
import certifi
from bs4 import Tag


async def fetch_page(url: str) -> str:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=ssl_context) as response:
            return await response.text()


def parse_object(fields: dict[str, callable], element: Tag | str) -> dict[str, str | int]:
    return {field: fields[field](element) for field in fields}
