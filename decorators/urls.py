from urllib.parse import urlparse, ParseResult

from static import BASE_URL


def prepend_base_url(func: callable) -> callable:
    def wrapper(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        return ''.join([BASE_URL, result])

    return wrapper


def validate_url(func: callable) -> callable:
    def wrapper(*args, **kwargs) -> str:
        result: ParseResult = urlparse(str(func(*args, **kwargs)))
        if not all([result.scheme, result.netloc]):
            raise ValueError(f'Invalid URL {result.geturl()}')
        return result.geturl()

    return wrapper


def process_url(func: callable) -> callable:
    @validate_url
    @prepend_base_url
    def wrapper(*args, **kwargs) -> str:
        return func(*args, **kwargs)

    return wrapper
