from bs4 import Tag, BeautifulSoup

from data_models.study_program.css_selectors import STUDY_PROGRAMS_2023_LI_SELECTOR
from decorators.urls import process_url
from utils.data_and_parsing import parse_object, fetch_page
from data_models.study_program.model import StudyProgram
from data_models.study_program.css_selectors import (
    STUDY_PROGRAM_NAME_SELECTOR,
    STUDY_PROGRAM_DURATION_SELECTOR,
    STUDY_PROGRAM_URL_SELECTOR,
)
from static import STUDY_PROGRAMS_URL


def parse_study_program_name(element: Tag) -> str:
    return element.select_one(STUDY_PROGRAM_NAME_SELECTOR).text.strip()


def parse_study_program_duration(element: Tag) -> int:
    return int(element.select_one(STUDY_PROGRAM_DURATION_SELECTOR).text.strip())


@process_url
def parse_study_program_url(element: Tag) -> str:
    return element.select_one(STUDY_PROGRAM_URL_SELECTOR)['href']


parse_fields: dict[str, callable] = {
    'study_program_name': parse_study_program_name,
    'study_program_duration': parse_study_program_duration,
    'study_program_url': parse_study_program_url,
}


def parse_study_program(element: Tag) -> StudyProgram:
    return StudyProgram(
        **parse_object(parse_fields, element)
    )


async def parse_study_program_data() -> list[StudyProgram]:
    html: str = await fetch_page(STUDY_PROGRAMS_URL)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    study_programs: list[Tag] = soup.select(STUDY_PROGRAMS_2023_LI_SELECTOR)
    return [parse_study_program(study_program) for study_program in study_programs]
