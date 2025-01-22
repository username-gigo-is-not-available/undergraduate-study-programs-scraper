from functools import wraps

from src.config import Config


def prepend_base_url(func: callable) -> callable:
    @wraps(func)
    def wrapper_prepend_base_url(*args, **kwargs) -> str:
        result: str = str(func(*args, **kwargs))
        return ''.join([Config.BASE_URL, result])

    return wrapper_prepend_base_url
