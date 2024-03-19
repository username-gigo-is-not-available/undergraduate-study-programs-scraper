from bs4 import Tag

from settings import BASE_URL


def prepend_base_url(url: str) -> str:
    return ''.join([BASE_URL, url])


def parse_object(fields: dict[str, callable], element: Tag | str) -> dict[str, str | int]:
    return {field: fields[field](element) for field in fields}
