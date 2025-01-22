from functools import wraps
from urllib.parse import ParseResult, urlparse

from src.patterns.validator.course import CourseValidator


def validate_course(func: callable) -> callable:
    @wraps(func)
    def wrapper_validate_course(*args, **kwargs):
        fields: dict[str, str] = func(*args, **kwargs)
        return CourseValidator.validate_course(fields)

    return wrapper_validate_course


def validate_url(func: callable) -> callable:
    @wraps(func)
    def wrapper_validate_url(*args, **kwargs) -> str:
        result: ParseResult = urlparse(str(func(*args, **kwargs)))
        if not all([result.scheme, result.netloc]):
            raise ValueError(f'Invalid URL {result.geturl()}')
        return result.geturl()

    return wrapper_validate_url
