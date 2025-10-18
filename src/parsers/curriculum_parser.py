import asyncio
import logging
import queue
import threading
from asyncio import Task
from concurrent.futures import Executor
from functools import partial, reduce
from http import HTTPStatus
from ssl import SSLContext
from typing import NamedTuple

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import DatasetConfiguration
from src.models.enums import CourseType
from src.models.named_tuples import Curriculum, StudyProgram, CourseHeader
from src.network import HTTPClient
from src.parsers.base_parser import Parser
from src.parsers.study_program_parser import StudyProgramParser
from src.corrector import CourseCorrector
from src.storage import IcebergClient


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}

    MANDATORY_COURSE_SECTION_SELECTOR: str = '.col-md-6.col-sm-12'
    ELECTIVE_COURSE_SECTION_SELECTOR: str = '.col-md-12.col-sm-12'
    COURSE_SECTION_ROWS_SELECTOR: str = 'tr'
    COURSE_CODE_SELECTOR: str = 'td:nth-child(1)'
    COURSE_NAME_AND_URL_SELECTOR: str = 'td:nth-child(2) > a'
    COURSE_SEMESTER_SELECTOR: str = 'td:nth-child(3)'
    MANDATORY_COURSE_SEMESTER_SELECTOR: str = 'h3 > span'

    COURSE_HEADERS_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_DONE_EVENT: asyncio.Event = asyncio.Event()
    COURSE_HEADERS_READY_EVENT: threading.Event = threading.Event()

    def parse_row(self, *args, **kwargs) -> Curriculum:

        study_program: StudyProgram = kwargs.get('study_program')
        course_row: Tag = kwargs.get('element')
        course_type: CourseType = kwargs.get('course_type')

        fields: dict[str, str | int] = CourseCorrector.correct({
            'course_code': self.extract_text(course_row, self.COURSE_CODE_SELECTOR),
            'course_name_mk': self.extract_text(course_row, self.COURSE_NAME_AND_URL_SELECTOR),
            'course_url': self.extract_url(course_row, self.COURSE_NAME_AND_URL_SELECTOR),
            'course_type': course_type,
            'course_semester': int(self.extract_text(course_row, self.COURSE_SEMESTER_SELECTOR))
        })
        course_header: CourseHeader = CourseHeader(
            course_code=fields.get('course_code'),
            course_name_mk=fields.get('course_name_mk'),
            course_url=fields.get('course_url'),
        )

        self.COURSE_HEADERS_QUEUE.put_nowait(course_header)
        if not self.COURSE_HEADERS_READY_EVENT.is_set():
            self.COURSE_HEADERS_READY_EVENT.set()

        curriculum: Curriculum = Curriculum(**{**study_program._asdict(), **fields})
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    def parse_data(self, *args, **kwargs) -> list[Curriculum]:

        def parse_section(study_program: StudyProgram, course_type: CourseType, section: Tag) -> list[Curriculum]:
            def filter_course_rows(row: Tag) -> bool:
                return bool(row.select_one(self.COURSE_CODE_SELECTOR) and row.select_one(self.COURSE_NAME_AND_URL_SELECTOR))

            def modify_course_row(row: Tag, tag_name: str, text: str) -> Tag:
                tag: Tag = Tag(name=tag_name)
                tag.string = text
                row.append(tag)
                return row

            filtered_course_rows: list[Tag] = list(filter(filter_course_rows, section.select(self.COURSE_SECTION_ROWS_SELECTOR)))
            augmented_course_rows: list[Tag] = [modify_course_row(row, 'td', self.extract_text(section, self.MANDATORY_COURSE_SEMESTER_SELECTOR))
                                      if course_type == CourseType.MANDATORY else row for row in filtered_course_rows]

            return [
                self.parse_row(study_program=study_program, element=course_row, course_type=course_type)
                for course_row in augmented_course_rows
            ]

        study_program: StudyProgram = kwargs.get('study_program')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = self.get_parsed_html(page_content)
        nested_curricula: list[list[Curriculum]] = []

        for section in soup.select(self.MANDATORY_COURSE_SECTION_SELECTOR):
            nested_curricula.append(parse_section(study_program, CourseType.MANDATORY, section))
        for section in soup.select(self.ELECTIVE_COURSE_SECTION_SELECTOR):
            nested_curricula.append(parse_section(study_program, CourseType.ELECTIVE, section))

        return reduce(lambda x, y: x + y, nested_curricula)


    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  dataset_configuration: DatasetConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  executor: Executor | None = None) -> list[NamedTuple]:

        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.wait()
        tasks: list[Task[tuple[int, str, StudyProgram]]] = []
        while not StudyProgramParser.STUDY_PROGRAMS_QUEUE.empty():
            study_program: StudyProgram = StudyProgramParser.STUDY_PROGRAMS_QUEUE.get_nowait()
            tasks.append(asyncio.create_task(
                    http_client.fetch_page_wrapper(session=session,
                                    ssl_context=ssl_context,
                                    url=study_program.study_program_url,
                                    named_tuple=study_program,
                                    )
                         ))
        for task in asyncio.as_completed(tasks):
            http_status, page_content, study_program = await task
            if http_status != HTTPStatus.OK:
                logging.error(
                    f"Tried to fetch {study_program.study_program_url} but got HTTP status: {http_status}"
                )
                break
            self.CURRICULA_QUEUE.put_nowait(loop.run_in_executor(executor, partial(self.parse_data, study_program=study_program, page_content=page_content)))

        nested_curricula: list[list[Curriculum]] = await asyncio.gather(
            *[self.CURRICULA_QUEUE.get_nowait() for _ in range(self.CURRICULA_QUEUE.qsize())])  # type: ignore
        self.CURRICULA_DONE_EVENT.set()
        curricula: list[Curriculum] = reduce(lambda x, y: x + y, nested_curricula)
        logging.info(f"Finished processing {dataset_configuration}")
        await iceberg_client.save_data(curricula, dataset_configuration)
        return curricula
