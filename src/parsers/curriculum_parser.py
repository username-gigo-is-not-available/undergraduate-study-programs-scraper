import asyncio
import logging
import threading
from asyncio import Event, Task
from collections import defaultdict
from concurrent.futures import Executor
from functools import cache, partial
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.config import Config
from src.models.enums import CourseType
from src.models.named_tuples import PartialCurriculum, StudyProgram, CourseHeader, Course, Curriculum
from src.parsers.base_parser import Parser
from src.parsers.field_parser import FieldParser
from src.parsers.study_program_parser import StudyProgramParser


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}
    CURRICULA_DATA_OUTPUT_FILE_NAME: Path = Config.CURRICULA_DATA_OUTPUT_FILE_NAME
    COURSE_HEADER_TABLES_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_HEADER_TABLE_ROWS_SELECTOR: str = 'tr'
    COURSE_HEADER_CODE_SELECTOR: str = 'td:nth-child(1)'
    COURSE_HEADER_NAME_AND_URL_SELECTOR: str = 'td:nth-child(2) > a'
    COURSE_HEADERS_SET: set[CourseHeader] = set()
    COURSE_HEADERS_QUEUE: Queue = Queue()
    COURSE_HEADERS_READY_EVENT: Event = Event()
    CURRICULUM_SUGGESTED_SEMESTER_SELECTOR: str = 'td:nth-child(3)'
    CURRICULUM_ELECTIVE_GROUP_SELECTOR: str = 'td:nth-child(4)'
    PARTIAL_CURRICULA_QUEUE: Queue = Queue()
    PARTIAL_CURRICULA_DONE_EVENT: Event = Event()
    LOCK: threading.Lock = threading.Lock()
    COURSE_HEADERS_LOCK: threading.Lock = threading.Lock()

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
    async def parse_row(cls, *args, **kwargs) -> PartialCurriculum:
        async def add_course_header(course_header: CourseHeader) -> CourseHeader:
            cls.COURSE_HEADERS_QUEUE.put_nowait(course_header)
            if not cls.COURSE_HEADERS_READY_EVENT.is_set():
                cls.COURSE_HEADERS_READY_EVENT.set()
            return course_header

        study_program: StudyProgram = kwargs.get('study_program')
        course_row: Tag = kwargs.get('element')
        fields: dict[str, str | int | bool] = cls.parse_fields(element=course_row)
        course_header: CourseHeader = CourseHeader(course_code=fields.get('course_code'),
                                                   course_name_mk=fields.get('course_name_mk'),
                                                   course_url=fields.get('course_url'))

        await add_course_header(course_header=course_header)

        partial_curriculum: PartialCurriculum = PartialCurriculum(
            *study_program,
            *course_header,
            course_type=CourseType.from_bool(fields.get('course_type'))
        )
        logging.info(f"Scraped partial curriculum {partial_curriculum}")

        return partial_curriculum

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[PartialCurriculum]:
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
        tasks: list[Task[PartialCurriculum]] = [asyncio.create_task(cls.parse_row(study_program=study_program, element=course_row))
                                                for course_row in course_rows]

        return await asyncio.gather(*tasks)  # type: ignore

    @classmethod
    async def process_data(cls, executor: Executor) -> list[PartialCurriculum]:
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        await StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.wait()

        while True:
            async with cls.async_lock(cls.LOCK, executor):
                if StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.is_set() and StudyProgramParser.STUDY_PROGRAMS_QUEUE.empty():
                    cls.PARTIAL_CURRICULA_DONE_EVENT.set()
                    partial_curricula: list[list[PartialCurriculum]] = await asyncio.gather(
                        *[cls.PARTIAL_CURRICULA_QUEUE.get_nowait() for _ in range(cls.PARTIAL_CURRICULA_QUEUE.qsize())])  # type: ignore
                    partial_curricula: list[PartialCurriculum] = [element for row in partial_curricula for element in row]
                    logging.info("Finished processing partial curricula")
                    return partial_curricula

            while not StudyProgramParser.STUDY_PROGRAMS_QUEUE.empty():
                study_program: StudyProgram = StudyProgramParser.STUDY_PROGRAMS_QUEUE.get_nowait()
                cls.PARTIAL_CURRICULA_QUEUE.put_nowait(loop.run_in_executor(executor, partial(cls.run_parse_data, item=study_program)))

    @classmethod
    async def merge_and_save_data(cls, partial_curricula: list[PartialCurriculum], courses: list[Course]) -> list[Curriculum]:
        curricula = defaultdict(list)
        for partial_curriculum in partial_curricula:
            curricula[partial_curriculum.course_code].append(partial_curriculum)

        for course in courses:
            course_code: str = course.course_code
            partial_curricula: list[PartialCurriculum] = curricula.get(course_code)
            curricula[course_code] = list(map(lambda partial_curriculum: Curriculum(**{**partial_curriculum._asdict(), **course._asdict()}), partial_curricula))

        result = [element for row in curricula.values() for element in row]
        await cls.save_data(result, cls.CURRICULA_DATA_OUTPUT_FILE_NAME, list(Curriculum._fields))

        return result
