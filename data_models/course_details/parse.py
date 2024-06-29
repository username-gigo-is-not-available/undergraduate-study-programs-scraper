import logging
from functools import cache

from bs4 import BeautifulSoup, Tag
from data_models.course_details.css_selectors import (
    COURSE_DETAILS_PREREQUISITE_SELECTOR,
    COURSE_DETAILS_PROFESSORS_SELECTOR,
    COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR,
    COURSE_DETAILS_CODE_SELECTOR,
    COURSE_DETAILS_NAME_MK_SELECTOR,
    COURSE_TABLES_CLASS_NAME, COURSE_DETAILS_SEMESTER_SEASON_SELECTOR, COURSE_DETAILS_NAME_EN_SELECTOR
)
from data_models.course_header.model import CourseHeader
from data_models.course_details.enums import CourseSeason
from data_models.course_details.model import CourseDetails
from utils.data_and_parsing import parse_object, fetch_page


def get_course_table(soup: BeautifulSoup) -> Tag:
    return soup.select_one(COURSE_TABLES_CLASS_NAME)


def parse_course_code(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_CODE_SELECTOR).text.strip()


def parse_course_name_mk(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_NAME_MK_SELECTOR).text.strip()


def parse_course_name_en(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_NAME_EN_SELECTOR).text.strip()


@cache
def parse_academic_year(course_table: Tag) -> int:
    return int(course_table.select_one(COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR).text.strip())


@cache
def parse_course_season(course_table: Tag) -> str:
    return CourseSeason(course_table.select_one(COURSE_DETAILS_SEMESTER_SEASON_SELECTOR).text.capitalize()).value


@cache
def parse_course_professors(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_PROFESSORS_SELECTOR).text.strip()


@cache
def parse_course_prerequisite(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_PREREQUISITE_SELECTOR).text.strip()


parse_fields: dict[str, callable] = {
    'course_code': parse_course_code,
    'course_name_mk': parse_course_name_mk,
    'course_name_en': parse_course_name_en,
    'course_academic_year': parse_academic_year,
    'course_season': parse_course_season,
    'course_professors': parse_course_professors,
    'course_prerequisite': parse_course_prerequisite
}


def parse_course_details(course_table: Tag) -> CourseDetails:
    course_details: CourseDetails = CourseDetails(**parse_object(parse_fields, course_table))
    logging.info(f"Scraped course details {course_details}")
    return course_details


async def parse_course_details_data(course_headers: list[CourseHeader]) -> list[CourseDetails]:
    result: list[CourseDetails] = []
    for course_header in course_headers:
        html: str = await fetch_page(course_header.course_url)
        soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
        course_details: CourseDetails = parse_course_details(get_course_table(soup))
        result.append(course_details)
    return result
