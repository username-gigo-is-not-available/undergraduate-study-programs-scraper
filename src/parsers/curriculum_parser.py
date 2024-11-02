import asyncio
import logging
import threading
from asyncio import Event, Task
from concurrent.futures import Executor
from functools import cache
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.enums import CourseType
from src.parsers.base_parser import Parser
from src.parsers.field_parser import FieldParser
from src.models import Curriculum, StudyProgram, CourseHeader
from src.parsers.study_program_parser import StudyProgramParser
from src.config import Config


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}
    CURRICULA_DATA_OUTPUT_FILE_NAME: Path = Config.CURRICULA_DATA_OUTPUT_FILE_NAME
    COURSE_HEADER_TABLES_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_HEADER_TABLE_ROWS_SELECTOR: str = 'tr'
    COURSE_HEADER_CODE_SELECTOR: str = 'td:nth-child(1)'
    COURSE_HEADER_NAME_AND_URL_SELECTOR: str = 'td:nth-child(2) > a'
    COURSE_HEADERS_SET: set[CourseHeader] = set()
    COURSE_HEADERS_QUEUE: Queue = Queue()
    COURSE_HEADERS_READY: Event = Event()
    CURRICULUM_SUGGESTED_SEMESTER_SELECTOR: str = 'td:nth-child(3)'
    CURRICULUM_ELECTIVE_GROUP_SELECTOR: str = 'td:nth-child(4)'
    CURRICULA_QUEUE: Queue = Queue()
    CURRICULA_DONE: Event = Event()
    CURRICULA_DONE_MESSAGE: str = "Finished processing curricula"
    LOCK: threading.Lock = threading.Lock()

    @classmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        return [
            FieldParser('course_code', cls.COURSE_HEADER_CODE_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_name_mk', cls.COURSE_HEADER_NAME_AND_URL_SELECTOR, element,
                        FieldParser.parse_text_field),
            FieldParser('course_url', cls.COURSE_HEADER_NAME_AND_URL_SELECTOR, element,
                        FieldParser.parse_url_field),
            FieldParser('course_type',
                        [
                            CurriculumParser.CURRICULUM_ELECTIVE_GROUP_SELECTOR,
                            CurriculumParser.CURRICULUM_SUGGESTED_SEMESTER_SELECTOR
                        ],
                        element,
                        FieldParser.check_if_fields_exists)
        ]

    @classmethod
    @cache
    async def parse_row(cls, *args, **kwargs) -> Curriculum:
        async def add_course_header(course_header: CourseHeader) -> CourseHeader:
            if course_header not in cls.COURSE_HEADERS_SET:
                cls.COURSE_HEADERS_SET.add(course_header)
                cls.COURSE_HEADERS_QUEUE.put_nowait(course_header)
                cls.COURSE_HEADERS_READY.set()
                return course_header

        study_program: StudyProgram = kwargs.get('study_program')
        course_row: Tag = kwargs.get('element')
        fields: dict[str, str | int | bool] = cls.parse_fields(element=course_row)
        course_header: CourseHeader = CourseHeader(course_code=fields.get('course_code'),
                                                   course_name_mk=fields.get('course_name_mk'),
                                                   course_url=fields.get('course_url'))

        await add_course_header(course_header=course_header)

        curriculum: Curriculum = Curriculum(
            *study_program,
            *course_header,
            course_type=CourseType.from_bool(fields.get('course_type'))
        )
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[Curriculum]:
        async def parse_course_rows(soup: BeautifulSoup) -> list[Tag]:
            def is_course_row(element: Tag) -> bool:
                field = FieldParser('is_course_row',
                                    [cls.COURSE_HEADER_NAME_AND_URL_SELECTOR, cls.COURSE_HEADER_CODE_SELECTOR],
                                    element,
                                    FieldParser.check_if_fields_exists)
                return field()

            course_tables: list[Tag] = soup.select(cls.COURSE_HEADER_TABLES_CLASS_NAME)
            course_table_rows: list[Tag] = [row for course_table in course_tables for row in
                                            course_table.select(cls.COURSE_HEADER_TABLE_ROWS_SELECTOR)]
            return list(filter(is_course_row, course_table_rows))

        study_program: StudyProgram = kwargs.get('item')
        soup: BeautifulSoup = await cls.get_parsed_html(study_program.study_program_url)
        course_rows: list[Tag] = await parse_course_rows(soup=soup)
        tasks: list[Task[Curriculum]] = [asyncio.create_task(cls.parse_row(study_program=study_program, element=course_row))
                                         for course_row in course_rows]

        curricula: list[Curriculum] = [_ for _ in await asyncio.gather(*tasks)]

        return curricula

    @classmethod
    async def scrape_and_save_data(cls,
                                   executor: Executor = None,
                                   *args,
                                   **kwargs
                                   ) -> list[Curriculum]:
        data: list[Curriculum] = await super().scrape_and_save_data(
            file_name=cls.CURRICULA_DATA_OUTPUT_FILE_NAME,
            column_order=list(Curriculum._fields),
            output_event=cls.CURRICULA_DONE,
            output_queue=cls.CURRICULA_QUEUE,
            executor=executor,
            parse_func=cls.run_parse_data,
            done_log_msg=cls.CURRICULA_DONE_MESSAGE,
            lock=cls.LOCK,
            input_event=StudyProgramParser.STUDY_PROGRAMS_DONE_EVENT,
            input_queue=StudyProgramParser.STUDY_PROGRAMS_QUEUE,
            flatten=True,
            *args,
            **kwargs
        )
        return data
