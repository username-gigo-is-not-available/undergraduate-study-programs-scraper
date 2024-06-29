import logging
from functools import cache

from bs4 import Tag, BeautifulSoup

from data_models.study_program.model import StudyProgram

from decorators.urls import process_url
from data_models.course_header.enums import CourseType
from data_models.course_header.model import CourseHeader
from utils.filtering import is_course_row
from utils.data_and_parsing import parse_object, fetch_page
from data_models.course_header.css_selectors import (
    COURSE_TABLES_CLASS_NAME,
    COURSE_TABLE_ROWS_SELECTOR,
    COURSE_HEADER_CODE_SELECTOR,
    COURSE_HEADER_NAME_AND_URL_SELECTOR,
    COURSE_HEADER_ELECTIVE_GROUP_SELECTOR,
    COURSE_HEADER_SUGGESTED_SEMESTER_SELECTOR,
)


@cache
def get_course_tables(soup: BeautifulSoup) -> list[Tag]:
    return soup.select(COURSE_TABLES_CLASS_NAME)


@cache
def get_course_table_rows(course_table: Tag) -> list[Tag]:
    return course_table.select(COURSE_TABLE_ROWS_SELECTOR)


def get_course_tables_rows_flattened(course_tables: list[Tag]) -> list[Tag]:
    return [
        row for course_table in course_tables
        for row in get_course_table_rows(course_table)
    ]


@cache
def parse_course_code(course_row: Tag) -> str:
    return course_row.select_one(COURSE_HEADER_CODE_SELECTOR).text.strip()


@cache
def parse_course_name_mk(course_row: Tag) -> str:
    return course_row.select_one(COURSE_HEADER_NAME_AND_URL_SELECTOR).text.strip()


@process_url
@cache
def parse_course_url(course_row: Tag) -> str:
    return course_row.select_one(COURSE_HEADER_NAME_AND_URL_SELECTOR)['href']


@cache
def parse_course_type(course_row: Tag) -> str:
    elective_group: Tag = course_row.select_one(COURSE_HEADER_ELECTIVE_GROUP_SELECTOR)
    suggested_semester: Tag = course_row.select_one(COURSE_HEADER_SUGGESTED_SEMESTER_SELECTOR)
    return CourseType.ELECTIVE.value if elective_group and suggested_semester else CourseType.MANDATORY.value


parse_fields: dict[str, callable] = {
    'course_code': parse_course_code,
    'course_name_mk': parse_course_name_mk,
    'course_url': parse_course_url,
    'course_type': parse_course_type,
}


@cache
def parse_course_header(course_row: Tag) -> CourseHeader:
    course_header: CourseHeader = CourseHeader(**parse_object(parse_fields, course_row))
    logging.info(f"Scraped course header {course_header}")
    return course_header


async def parse_course_headers_data(study_program: StudyProgram) -> list[CourseHeader]:
    html: str = await fetch_page(study_program.study_program_url)
    soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
    logging.info(f"Getting curriculum data {study_program}")
    course_tables: list[Tag] = get_course_tables(soup)
    return [parse_course_header(course_row) for course_row in get_course_tables_rows_flattened(course_tables) if
            is_course_row(course_row)]
