from bs4 import Tag

from constants import (
    STUDY_PROGRAM_NAME_SELECTOR,
    STUDY_PROGRAM_DURATION_SELECTOR,
    STUDY_PROGRAM_URL_SELECTOR,
)
from models import StudyProgram
from extract.utils import prepend_base_url


def parse_study_program_name(element: Tag) -> str:
    return element.select_one(STUDY_PROGRAM_NAME_SELECTOR).text.strip()


def parse_study_program_duration(element: Tag) -> int:
    return int(element.select_one(STUDY_PROGRAM_DURATION_SELECTOR).text.strip())


def parse_study_program_url(element: Tag) -> str:
    return prepend_base_url(element.select_one(STUDY_PROGRAM_URL_SELECTOR)['href'])


parse_fields = {
    'name': parse_study_program_name,
    'duration': parse_study_program_duration,
    'url': parse_study_program_url,
}


def parse_study_program(element: Tag) -> StudyProgram:
    return StudyProgram(
        **{field: parse_fields[field](element) for field in parse_fields}
    )
