import logging
from functools import wraps
from urllib.parse import urlparse, ParseResult

import requests
from requests import Response

from static import BASE_URL


def check_if_url_is_accessible(func):
    @wraps(func)
    def wrapper_check_if_base_url_is_accessible(*args, **kwargs):
        logging.info(f"Checking if base URL is accessible: {BASE_URL}")
        response: Response = requests.get(BASE_URL)
        if response.status_code == 200:
            logging.info(f"Base URL is accessible: {BASE_URL}")
            return func(*args, **kwargs)
        else:
            raise ConnectionError(f"Base URL is not accessible. Status code: {response.status_code}")

    return wrapper_check_if_base_url_is_accessible


def prepend_base_url(func: callable) -> callable:
    @wraps(func)
    def wrapper_prepend_base_url(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        return ''.join([BASE_URL, result])

    return wrapper_prepend_base_url


def validate_url(func: callable) -> callable:
    @wraps(func)
    def wrapper_validate_url(*args, **kwargs) -> str:
        result: ParseResult = urlparse(str(func(*args, **kwargs)))
        if not all([result.scheme, result.netloc]):
            raise ValueError(f'Invalid URL {result.geturl()}')
        return result.geturl()

    return wrapper_validate_url


def process_url(func: callable) -> callable:
    @validate_url
    @prepend_base_url
    @wraps(func)
    def wrapper_process_url(*args, **kwargs) -> str:
        return func(*args, **kwargs)

    return wrapper_process_url
