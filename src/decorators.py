from functools import wraps
from urllib.parse import urlparse, ParseResult

from src.config import Config
from src.patterns.validator.course import CourseValidator


def prepend_base_url(func: callable) -> callable:
    @wraps(func)
    def wrapper_prepend_base_url(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        return ''.join([Config.BASE_URL, result])

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


def validate_course(func: callable) -> callable:
    @wraps(func)
    def wrapper_validate_course(*args, **kwargs):
        fields: dict[str, str] = func(*args, **kwargs)
        return CourseValidator.validate_course(fields)

    return wrapper_validate_course
