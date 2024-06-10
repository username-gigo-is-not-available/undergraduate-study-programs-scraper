from urllib.parse import urlparse
from settings import BASE_URL, INVALID_COURSE_CODES


def clean_whitespace(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, str):
            cleaned_result = ' '.join(result.split())
            return cleaned_result
        return result

    return wrapper


def prepend_base_url(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = str(func(*args, **kwargs))
        return ''.join([BASE_URL, result])

    return wrapper


def validate_url(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = urlparse(str(func(*args, **kwargs)))
        if not all([result.scheme, result.netloc]):
            raise ValueError(f'Invalid URL {result.geturl()}')
        return result.geturl()

    return wrapper


def process_url(func: callable) -> callable:
    @validate_url
    @prepend_base_url
    def wrapped(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapped


def validate_course(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        course = func(*args, **kwargs)
        if course.code in INVALID_COURSE_CODES:
            code = ''.join(course.name.split(' ')[0])
            name = ' '.join(course.name.split(' ')[1:])
            common_args = {'code': code, 'name': name}
            other_args = {key: value for key, value in course._asdict().items() if key not in common_args}
            return type(course)(**common_args, **other_args)
        return course

    return wrapper