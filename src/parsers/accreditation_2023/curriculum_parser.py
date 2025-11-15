import asyncio
import logging
import queue
import threading
from concurrent.futures import Executor
from functools import partial, reduce
from ssl import SSLContext
from typing import Any

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import TableConfiguration
from src.models.accreditation_2023.data_classes import Curriculum2023
from src.models.enums import OfferingType
from src.network import HTTPClient
from src.parsers.accreditation_2023.study_program_parser import StudyProgramParser
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}

    CURRICULUM_ACCREDITATION_YEAR: int = 2023
    MANDATORY_COURSE_SECTION_SELECTOR: str = '.col-md-6.col-sm-12'
    ELECTIVE_COURSE_SECTION_SELECTOR: str = '.col-md-12.col-sm-12'
    COURSE_SECTION_ROWS_SELECTOR: str = 'tr'
    COURSE_URL_SELECTOR: str = 'td:nth-child(2) > a'
    COURSE_SEMESTER_SELECTOR: str = 'td:nth-child(3)'
    MANDATORY_COURSE_SEMESTER_SELECTOR: str = 'h3 > span'

    COURSE_URLS_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_DONE_EVENT: asyncio.Event = asyncio.Event()
    COURSE_URLS_READY_EVENT: threading.Event = threading.Event()

    def parse_row(self, *args, **kwargs) -> Curriculum2023:

        study_program_url: str = kwargs.get('study_program_url')
        element: Tag = kwargs.get('element')
        offering_type: OfferingType = kwargs.get('offering_type')
        course_url: str = self.extract_url(element, self.COURSE_URL_SELECTOR)

        self.COURSE_URLS_QUEUE.put_nowait(course_url)
        if not self.COURSE_URLS_READY_EVENT.is_set():
            self.COURSE_URLS_READY_EVENT.set()

        curriculum: Curriculum2023 = Curriculum2023(
            accreditation_year=self.CURRICULUM_ACCREDITATION_YEAR,
            study_program_url=study_program_url,
            course_url=course_url,
            offering_type=offering_type,
            semester=int(self.extract_text(element, self.COURSE_SEMESTER_SELECTOR))
        )
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    @classmethod
    def _append_element(cls, row: Tag, element_name: str, text: str) -> Tag:
        tag: Tag = Tag(name=element_name)
        tag.string = text
        row.append(tag)
        return row

    @classmethod
    def _is_valid_course_row(cls, row: Tag):
        return bool(row.select_one(cls.COURSE_URL_SELECTOR))

    @classmethod
    def _flatten(cls, data: list[list[Any]]) -> list[Any]:
        return reduce(lambda x, y: x + y, data)

    @classmethod
    def _extract_course_rows_from_section(cls, section: Tag) -> list[Tag]:
        return [row for row in section.select(cls.COURSE_SECTION_ROWS_SELECTOR) if cls._is_valid_course_row(row)]

    @classmethod
    def _modify_course_rows(cls, rows: list[Tag], element_name: str, text: str) -> list[Tag]:
        return [cls._append_element(row, element_name, text) for row in rows]

    def _parse_course_rows(self, rows: list[Tag], offering_type: OfferingType, study_program_url: str) -> list[Curriculum2023]:
        return [self.parse_row(
                    study_program_url=study_program_url,
                    element=row,
                    offering_type=offering_type
                ) for row in rows]


    def parse_data(self, *args, **kwargs) -> list[Curriculum2023]:

        study_program_url: str = kwargs.get('study_program_url')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = self.get_parsed_html(page_content)

        curricula: list[Curriculum2023] = []
        offering_type_selectors: dict[OfferingType, str] = {
            OfferingType.MANDATORY: self.MANDATORY_COURSE_SECTION_SELECTOR,
            OfferingType.ELECTIVE: self.ELECTIVE_COURSE_SECTION_SELECTOR,
        }

        for offering_type, selector in offering_type_selectors.items():
            for section in soup.select(selector):
                rows: list[Tag] = self._extract_course_rows_from_section(section)
                if offering_type == OfferingType.MANDATORY:
                    course_semester: str = self.extract_text(section,
                                                            self.MANDATORY_COURSE_SEMESTER_SELECTOR
                                                            )
                    rows: list[Tag] = self._modify_course_rows(rows=rows, element_name='td', text=course_semester)
                curricula.extend(self._parse_course_rows(
                    rows=rows,
                    offering_type=offering_type,
                    study_program_url=study_program_url
                ))


        return curricula


    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Curriculum2023]:

        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.wait()
        while True:
            try:
                study_program, page_content = StudyProgramParser.STUDY_PROGRAMS_QUEUE.get_nowait()
            except queue.Empty:
                if StudyProgramParser.STUDY_PROGRAMS_DONE_EVENT.is_set():
                    break
                else:
                    await asyncio.sleep(0.1)
                    continue

            self.CURRICULA_QUEUE.put_nowait(loop.run_in_executor(executor, partial(self.parse_data,
                                                                                   study_program_url=study_program.url,
                                                                                   page_content=page_content)))

        nested_curricula: list[list[Curriculum2023]] = await asyncio.gather(
            *[self.CURRICULA_QUEUE.get_nowait() for _ in range(self.CURRICULA_QUEUE.qsize())])  # type: ignore
        self.CURRICULA_DONE_EVENT.set()
        curricula: list[Curriculum2023] = self._flatten(nested_curricula)
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(curricula, iceberg_configuration)
        return curricula
