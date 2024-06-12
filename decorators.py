from urllib.parse import urlparse
from settings import BASE_URL, INVALID_COURSE_CODES


def replace_nulls(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = str(func(*args, **kwargs))
        return result if result else 'нема'

    return wrapper


def clean_whitespace(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = str(func(*args, **kwargs))
        cleaned_result = ' '.join(result.split())
        return cleaned_result

    return wrapper


def clean_newlines(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        result = str(func(*args, **kwargs))
        cleaned_result = ', '.join(filter(lambda x: x, result.split('\n')))
        return cleaned_result

    return wrapper


def process_multivalued_field(func: callable) -> callable:
    @replace_nulls
    @clean_whitespace
    @clean_newlines
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

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
