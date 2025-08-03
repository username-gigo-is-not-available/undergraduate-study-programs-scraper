import asyncio
import logging
import threading
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.models.named_tuples import CourseDetails, CourseHeader
from src.parsers.base_parser import Parser
from src.parsers.curriculum_parser import CurriculumParser
from src.patterns.validator.schema import CourseValidator


class CourseParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSE_TABLE_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_NAME_MK_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
    COURSE_NAME_EN_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(2) > span'
    COURSE_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
    COURSE_URL_SELECTOR: str = 'head > link:nth-child(7)'
    COURSE_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3)'
    COURSE_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3)'
    COURSE_ACADEMIC_YEAR_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
    COURSE_SEMESTER_SEASON_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
    COURSE_COMPETENCE_SELECTOR: str =  'tr:nth-child(9) > td:nth-child(2) > p:nth-child(3)'
    COURSE_CONTENT_SELECTOR: str =  'tr:nth-child(10) > td:nth-child(2) > p:nth-child(3)'
    COURSES_QUEUE: Queue = Queue()
    COURSES_DONE_EVENT: asyncio.Event = asyncio.Event()
    COURSE_HEADERS_LOCK: threading.Lock = threading.Lock()
    PROCESSED_COURSE_HEADERS: set[CourseHeader] = set()
    CURRICULA_LOCK: threading.Lock = threading.Lock()

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('course_header')
        course_table: Tag = kwargs.get('element')

        fields: dict[str, str] = CourseValidator.validate_course({
            'course_name_en': cls.extract_text(course_table, cls.COURSE_NAME_EN_SELECTOR),
            'course_professors': cls.extract_text(course_table, cls.COURSE_PROFESSORS_SELECTOR),
            'course_prerequisites': cls.extract_text(course_table, cls.COURSE_PREREQUISITE_SELECTOR),
            'course_competence': cls.extract_text(course_table, cls.COURSE_COMPETENCE_SELECTOR),
            'course_content': cls.extract_text(course_table, cls.COURSE_CONTENT_SELECTOR),
        })

        course: CourseDetails = CourseDetails(**{**course_header._asdict(), **fields})
        logging.info(f"Scraped course {course}")
        return course

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('item')
        soup: BeautifulSoup = await Parser.get_parsed_html(course_header.course_url)
        course_table: Tag = soup.select_one(cls.COURSE_TABLE_CLASS_NAME)
        return await cls.parse_row(course_header=course_header, element=course_table)

    @classmethod
    async def process_and_save_data(cls, executor: Executor) -> list[CourseDetails]:

        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await CurriculumParser.COURSE_HEADERS_READY_EVENT.wait()

        while True:

            async with cls.async_lock(cls.CURRICULA_LOCK, executor):
                if CurriculumParser.COURSE_HEADERS_QUEUE.empty() and CurriculumParser.CURRICULA_DONE_EVENT.is_set():
                    cls.COURSES_DONE_EVENT.set()
                    courses: list[CourseDetails] = await asyncio.gather(
                        *[cls.COURSES_QUEUE.get_nowait() for _ in range(cls.COURSES_QUEUE.qsize())])  # type: ignore
                    logging.info("Finished processing courses")
                    await cls.save_data(courses, DatasetConfiguration.COURSES)
                    return courses

            while not CurriculumParser.COURSE_HEADERS_QUEUE.empty():
                async with cls.async_lock(cls.COURSE_HEADERS_LOCK, executor):
                    course_header: CourseHeader = CurriculumParser.COURSE_HEADERS_QUEUE.get_nowait()
                    if course_header not in cls.PROCESSED_COURSE_HEADERS:
                        cls.PROCESSED_COURSE_HEADERS.add(course_header)
                        cls.COURSES_QUEUE.put_nowait(loop.run_in_executor(executor, partial(cls.run_parse_data, item=course_header)))
