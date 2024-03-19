import logging

from bs4 import BeautifulSoup, Tag
from constants import (
    COURSE_DETAILS_PREREQUISITE_SELECTOR,
    COURSE_DETAILS_PROFESSORS_SELECTOR,
    COURSE_DETAILS_YEAR_SELECTOR,
    COURSE_DETAILS_CODE_SELECTOR,
    COURSE_DETAILS_NAME_SELECTOR,
    COURSE_TABLES_CLASS_NAME
)
from handlers import handle_invalid_course
from models import CourseDetails
from extract.utils import parse_object


def parse_course_table(soup: BeautifulSoup) -> Tag:
    return soup.select_one(COURSE_TABLES_CLASS_NAME)


def parse_course_year(course_table: Tag) -> int:
    return int(course_table.select_one(COURSE_DETAILS_YEAR_SELECTOR).text.strip())


def parse_course_professors(course_table: Tag) -> str:
    professors = course_table.select_one(COURSE_DETAILS_PROFESSORS_SELECTOR).text.strip().replace("\n", ",")
    return professors if professors else "нема"


def parse_course_prerequisite(course_table: Tag) -> str:
    prerequisite = course_table.select_one(COURSE_DETAILS_PREREQUISITE_SELECTOR).text.strip()
    return prerequisite if prerequisite else "нема"


def parse_course_code(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_CODE_SELECTOR).text.strip()


def parse_course_name(course_table: Tag) -> str:
    return course_table.select_one(COURSE_DETAILS_NAME_SELECTOR).text.strip()


def parse_course_semester(course_season: str, course_year: int) -> int:
    semester = course_year * 2
    return semester if course_season == 'SUMMER' else semester - 1


def parse_course_season(course_code: str) -> str:
    return 'WINTER' if course_code[3] == 'W' else 'SUMMER' if course_code[3] == 'L' else 'WINTER'


def parse_course_level(course_code: str) -> int:
    return int(course_code[4])


parse_fields_course_table = {
    'code': parse_course_code,
    'name': parse_course_name,
    'year': parse_course_year,
    'professors': parse_course_professors,
    'prerequisite': parse_course_prerequisite
}

parse_fields_course_code = {
    'level': parse_course_level,
    'season': parse_course_season,
}


def parse_course_details(course_table: Tag) -> CourseDetails:
    fields = parse_object(parse_fields_course_table, course_table)
    code = fields.get('code')
    fields.update(parse_object(parse_fields_course_code, code))
    fields.update({'semester': parse_course_semester(fields.get('season'), fields.get('year'))})
    course_details = handle_invalid_course(CourseDetails(**fields))
    logging.info(f"Scraped course details {course_details}")
    return course_details
