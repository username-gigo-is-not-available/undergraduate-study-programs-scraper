import logging

from bs4 import BeautifulSoup, Tag
from constants import (
    COURSE_DETAILS_PREREQUISITE_SELECTOR,
    COURSE_DETAILS_PROFESSORS_SELECTOR,
    COURSE_DETAILS_SEMESTER_NUMBER_SELECTOR,
    COURSE_DETAILS_CODE_SELECTOR,
    COURSE_DETAILS_NAME_SELECTOR,
    COURSE_TABLES_CLASS_NAME, COURSE_DETAILS_SEMESTER_SEASON_SELECTOR
)
from decorators import clean_whitespace, validate_course
from enums import CourseSeason
from models import CourseDetails
from parsers import parse_object


def get_course_table(soup: BeautifulSoup) -> Tag:
    return soup.select_one(COURSE_TABLES_CLASS_NAME)


@clean_whitespace
def parse_course_professors(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_PROFESSORS_SELECTOR).text


@clean_whitespace
def parse_course_prerequisite(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_PREREQUISITE_SELECTOR).text


@clean_whitespace
def parse_course_code(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_CODE_SELECTOR).text


@clean_whitespace
def parse_course_name(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_NAME_SELECTOR).text


def parse_course_semester(course_table: Tag) -> int:
    return int(course_table.select_one(COURSE_DETAILS_SEMESTER_NUMBER_SELECTOR).text.strip())


@clean_whitespace
def parse_course_season(course_table: Tag) -> str:
    return CourseSeason(course_table.select_one(COURSE_DETAILS_SEMESTER_SEASON_SELECTOR).text.capitalize()).value


parse_fields = {
    'code': parse_course_code,
    'name': parse_course_name,
    'semester': parse_course_semester,
    'season': parse_course_season,
    'professors': parse_course_professors,
    'prerequisite': parse_course_prerequisite
}


@validate_course
def parse_course_details(course_table: Tag) -> CourseDetails:
    course_details = CourseDetails(**parse_object(parse_fields, course_table))
    logging.info(f"Scraped course details {course_details}")
    return course_details
