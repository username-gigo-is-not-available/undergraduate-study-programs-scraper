from bs4 import Tag
from constants import COURSE_HEADER_CODE_SELECTOR, COURSE_HEADER_NAME_AND_URL_SELECTOR
from extract.study_program import StudyProgram


def is_macedonian_study_program(study_program: StudyProgram) -> bool:
    return study_program.url.endswith('mk')


def is_course_row(course_row: Tag) -> bool:
    return (course_row.select(COURSE_HEADER_NAME_AND_URL_SELECTOR) != []
            and course_row.select(COURSE_HEADER_CODE_SELECTOR) != [])
