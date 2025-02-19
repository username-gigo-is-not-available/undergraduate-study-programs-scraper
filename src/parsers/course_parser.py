import asyncio
import logging
import threading
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.config import Config
from src.models.enums import CourseSemesterSeasonType
from src.models.named_tuples import Course, CourseHeader
from src.parsers.base_parser import Parser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.field_parser import FieldParser


class CourseParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSES_DATA_OUTPUT_FILE_NAME: Path = Config.COURSES_DATA_OUTPUT_FILE_NAME
    COURSE_TABLE_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_NAME_MK_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
    COURSE_NAME_EN_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(2) > span'
    COURSE_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
    COURSE_URL_SELECTOR: str = 'head > link:nth-child(7)'
    COURSE_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3)'
    COURSE_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3)'
    COURSE_ACADEMIC_YEAR_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
    COURSE_SEMESTER_SEASON_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
    COURSES_QUEUE: Queue = Queue()
    COURSES_DONE_EVENT: asyncio.Event = asyncio.Event()
    LOCK: threading.Lock = threading.Lock()
    PROCESSED_COURSE_HEADERS: set[CourseHeader] = set()
    LOCK2: threading.Lock = threading.Lock()

    @classmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        return [
            FieldParser('course_name_en', CourseParser.COURSE_NAME_EN_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_semester_season', CourseParser.COURSE_SEMESTER_SEASON_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_academic_year', CourseParser.COURSE_ACADEMIC_YEAR_SELECTOR, element,
                        partial(FieldParser.parse_text_field, field_type=int)),
            FieldParser('course_professors', CourseParser.COURSE_PROFESSORS_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_prerequisites', CourseParser.COURSE_PREREQUISITE_SELECTOR, element,
                        FieldParser.parse_text_field),
        ]

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> Course:
        course_header: CourseHeader = kwargs.get('course_header')
        course_table: Tag = kwargs.get('element')
        fields: dict[str, str | int] = cls.parse_fields(element=course_table)
        course: Course = Course(
            *course_header,
            course_name_en=fields.get('course_name_en'),
            course_semester_season=CourseSemesterSeasonType.from_str(fields.get('course_semester_season')),
            course_academic_year=fields.get('course_academic_year'),
            course_professors=fields.get('course_professors'),
            course_prerequisites=fields.get('course_prerequisites')
        )
        logging.info(f"Scraped course details {course}")
        return course

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> Course:
        course_header: CourseHeader = kwargs.get('item')
        soup: BeautifulSoup = await Parser.get_parsed_html(course_header.course_url)
        course_table: Tag = soup.select_one(cls.COURSE_TABLE_CLASS_NAME)
        return await cls.parse_row(course_header=course_header, element=course_table)

    @classmethod
    async def process_and_save_data(cls, executor: Executor) -> list[Course]:

        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        await CurriculumParser.COURSE_HEADERS_READY_EVENT.wait()

        while True:

            async with cls.async_lock(cls.LOCK, executor):
                if CurriculumParser.COURSE_HEADERS_QUEUE.empty() and CurriculumParser.PARTIAL_CURRICULA_DONE_EVENT.is_set():
                    cls.COURSES_DONE_EVENT.set()
                    course_details: list[Course] = await asyncio.gather(
                        *[cls.COURSES_QUEUE.get_nowait() for _ in range(cls.COURSES_QUEUE.qsize())])  # type: ignore
                    logging.info("Finished processing courses")
                    await cls.save_data(course_details, cls.COURSES_DATA_OUTPUT_FILE_NAME, list(Course._fields))
                    return course_details

            while not CurriculumParser.COURSE_HEADERS_QUEUE.empty():
                async with cls.async_lock(cls.LOCK2, executor):
                    course_header: CourseHeader = CurriculumParser.COURSE_HEADERS_QUEUE.get_nowait()
                    if course_header not in cls.PROCESSED_COURSE_HEADERS:
                        cls.PROCESSED_COURSE_HEADERS.add(course_header)
                        cls.COURSES_QUEUE.put_nowait(loop.run_in_executor(executor, partial(cls.run_parse_data, item=course_header)))
