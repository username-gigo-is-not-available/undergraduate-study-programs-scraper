import asyncio
import logging
import threading
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.enums import ProcessingType, CourseSemesterSeasonType
from src.parsers.models.base_parser import Parser
from src.parsers.models.field_parser import FieldParser
from src.parsers.curriculum_parser import CurriculumParser
from src.models import CourseDetails, CourseHeader
from src.config import Config


class CourseDetailsParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSES_DATA_OUTPUT_FILE_NAME: Path = Config.COURSES_DATA_OUTPUT_FILE_NAME
    COURSE_TABLES_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_DETAILS_NAME_MK_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
    COURSE_DETAILS_NAME_EN_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(2) > span'
    COURSE_DETAILS_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
    COURSE_DETAILS_URL_SELECTOR: str = 'head > link:nth-child(7)'
    COURSE_DETAILS_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3)'
    COURSE_DETAILS_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3)'
    COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
    COURSE_DETAILS_SEMESTER_SEASON_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
    COURSE_DETAILS_QUEUE: Queue = Queue()
    COURSE_DETAILS_DONE_EVENT: asyncio.Event = asyncio.Event()
    COURSE_DETAILS_DONE_MESSAGE: str = "Finished processing course details"
    LOCK: threading.Lock = threading.Lock()
    PROCESSING_STRATEGY: ProcessingType = ProcessingType.CONSUMER

    @classmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        return [
            FieldParser('course_name_en', CourseDetailsParser.COURSE_DETAILS_NAME_EN_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_semester_season', CourseDetailsParser.COURSE_DETAILS_SEMESTER_SEASON_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_academic_year', CourseDetailsParser.COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR, element,
                        partial(FieldParser.parse_text_field, field_type=int)),
            FieldParser('course_professors', CourseDetailsParser.COURSE_DETAILS_PROFESSORS_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_prerequisites', CourseDetailsParser.COURSE_DETAILS_PREREQUISITE_SELECTOR, element,
                        FieldParser.parse_text_field),
        ]

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('course_header')
        course_table: Tag = kwargs.get('element')
        fields: dict[str, str | int] = cls.parse_fields(element=course_table)
        course_details: CourseDetails = CourseDetails(
            *course_header,
            course_name_en=fields.get('course_name_en'),
            course_semester_season=CourseSemesterSeasonType.from_str(fields.get('course_semester_season')),
            course_academic_year=fields.get('course_academic_year'),
            course_professors=fields.get('course_professors'),
            course_prerequisites=fields.get('course_prerequisites')
        )
        logging.info(f"Scraped course details {course_details}")
        return course_details

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('item')
        soup: BeautifulSoup = await Parser.get_parsed_html(course_header.course_url)
        course_table: Tag = soup.select_one(cls.COURSE_TABLES_CLASS_NAME)
        return await cls.parse_row(course_header=course_header, element=course_table)

    @classmethod
    async def process_and_save_data(cls, executor: Executor) -> list[CourseDetails]:
        data: list[CourseDetails] = await cls.get_processing_strategy(cls.PROCESSING_STRATEGY).process(
            parser_function=cls.run_parse_data,
            executor=executor,
            input_event=CurriculumParser.COURSE_HEADERS_READY_EVENT,
            input_queue=CurriculumParser.COURSE_HEADERS_QUEUE,
            lock=cls.LOCK,
            output_event=cls.COURSE_DETAILS_DONE_EVENT,
            output_queue=cls.COURSE_DETAILS_QUEUE,
            consumer_done_message=cls.COURSE_DETAILS_DONE_MESSAGE
        )
        await cls.save_data(data, cls.COURSES_DATA_OUTPUT_FILE_NAME, list(CourseDetails._fields))
        return data
