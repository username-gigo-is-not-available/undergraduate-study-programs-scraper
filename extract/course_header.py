import logging

from bs4 import Tag, BeautifulSoup

from constants import (
    COURSE_TABLE_ROWS_SELECTOR,
    COURSE_HEADER_CODE_SELECTOR,
    COURSE_HEADER_NAME_AND_URL_SELECTOR,
    COURSE_TABLES_CLASS_NAME,
    COURSE_HEADER_ELECTIVE_GROUP_SELECTOR,
    COURSE_HEADER_SUGGESTED_SEMESTER_SELECTOR
)
from handlers import handle_invalid_course
from models import CourseHeader
from extract.utils import parse_object, prepend_base_url


def parse_course_tables(soup: BeautifulSoup) -> list[Tag]:
    return soup.select(COURSE_TABLES_CLASS_NAME)


def parse_course_table_rows(course_table: Tag) -> list[Tag]:
    return course_table.select(COURSE_TABLE_ROWS_SELECTOR)


def parse_course_tables_rows(course_tables: list[Tag]) -> list[Tag]:
    return [
        row for course_table in course_tables
        for row in parse_course_table_rows(course_table)
    ]


def parse_course_code(course_row: Tag) -> str:
    return course_row.select_one(COURSE_HEADER_CODE_SELECTOR).text.strip()


def parse_course_name(course_row: Tag) -> str:
    return course_row.select_one(COURSE_HEADER_NAME_AND_URL_SELECTOR).text.strip()


def parse_course_url(course_row: Tag) -> str:
    return prepend_base_url(course_row.select_one(COURSE_HEADER_NAME_AND_URL_SELECTOR)['href'])


def parse_course_type(course_row: Tag) -> str:
    elective_group = course_row.select_one(COURSE_HEADER_ELECTIVE_GROUP_SELECTOR)
    suggested_semester = course_row.select_one(COURSE_HEADER_SUGGESTED_SEMESTER_SELECTOR)
    return 'ELECTIVE' if elective_group and suggested_semester else 'MANDATORY'


parse_fields = {
    'code': parse_course_code,
    'name': parse_course_name,
    'url': parse_course_url,
    'type': parse_course_type,
}


def parse_course_header(course_row: Tag) -> CourseHeader:
    course_header = handle_invalid_course(
        CourseHeader(**parse_object(parse_fields, course_row))
    )
    logging.info(f"Scraped course header {course_header}")
    return course_header
