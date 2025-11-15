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
from src.models.accreditation_2018.data_classes import Curriculum2018, StudyProgram2018
from src.models.enums import OfferingType
from src.network import HTTPClient
from src.parsers.accreditation_2018.study_program_parser import StudyProgramParser
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}

    CURRICULUM_ACCREDITATION_YEAR: int = 2018
    COURSE_SECTION_SELECTOR: str = 'div > div > div > div > div> div.view-grouping'
    COURSE_TABLE_SELECTOR: str = 'table'
    COURSE_TABLE_ROWS_SELECTOR: str = 'tbody > tr'
    COURSE_NAME_SELECTOR: str = 'td:nth-child(1)'
    COURSE_URL_SELECTOR: str = 'td:nth-child(1) > a'
    COURSE_SEMESTER_SELECTOR: str = 'div.view-grouping-header'
    COURSE_OFFERING_TYPE_SELECTOR: str = 'table > caption'
    COURSE_URLS_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_QUEUE: queue.Queue = queue.Queue()
    CURRICULA_DONE_EVENT: asyncio.Event = asyncio.Event()
    COURSE_URLS_READY_EVENT: threading.Event = threading.Event()

    def parse_row(self, *args, **kwargs) -> Curriculum2018:

        study_program: StudyProgram2018 = kwargs.get('study_program')
        element: Tag = kwargs.get('element')
        offering_type: OfferingType = kwargs.get('offering_type')
        semester: str = kwargs.get('semester')
        course_url: str = self.extract_url(element, self.COURSE_URL_SELECTOR)
        course_name: str = self.extract_text(element, self.COURSE_NAME_SELECTOR)
        self.COURSE_URLS_QUEUE.put_nowait(course_url)
        if not self.COURSE_URLS_READY_EVENT.is_set():
            self.COURSE_URLS_READY_EVENT.set()

        curriculum: Curriculum2018 = Curriculum2018(
            accreditation_year=self.CURRICULUM_ACCREDITATION_YEAR,
            study_program_full_name=study_program.full_name,
            course_name=course_name,
            offering_type=offering_type,
            semester=semester
        )
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    @classmethod
    def _is_valid_course_row(cls, row: Tag):
        return bool(row.select_one(cls.COURSE_URL_SELECTOR))

    @classmethod
    def _flatten(cls, data: list[list[Any]]) -> list[Any]:
        return reduce(lambda x, y: x + y, data)

    @classmethod
    def _extract_course_rows_from_table(cls, table: Tag) -> list[Tag]:
        return [row for row in table.select(cls.COURSE_TABLE_ROWS_SELECTOR) if cls._is_valid_course_row(row)]


    def _parse_course_rows(self,
                                rows: list[Tag],
                                offering_type: OfferingType,
                                semester: str,
                                study_program: StudyProgram2018
                           ):
        return [self.parse_row(
                        element=row,
                        offering_type=offering_type,
                        semester=semester,
                        study_program=study_program
                ) for row in rows]


    def parse_data(self, *args, **kwargs) -> list[Curriculum2018]:

        study_program: StudyProgram2018 = kwargs.get('study_program')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = self.get_parsed_html(page_content)

        curricula: list[Curriculum2018] = []
        sections: list[Tag] = soup.select(self.COURSE_SECTION_SELECTOR)

        for section in sections:
            semester: str =  self.extract_text(section, self.COURSE_SEMESTER_SELECTOR)
            tables: list[Tag] = section.select(self.COURSE_TABLE_SELECTOR)
            for table in tables:
                offering_type: OfferingType = OfferingType.from_string(self.extract_text(table, self.COURSE_OFFERING_TYPE_SELECTOR))
                rows: list[Tag] = self._extract_course_rows_from_table(table)
                curricula.extend(
                    self._parse_course_rows(
                        rows=rows,
                        offering_type=offering_type,
                        semester=semester,
                        study_program=study_program
                    )
                )

        return curricula


    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Curriculum2018]:

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
                                                                                   study_program=study_program,
                                                                                   page_content=page_content)))

        nested_curricula: list[list[Curriculum2018]] = await asyncio.gather(
            *[self.CURRICULA_QUEUE.get_nowait() for _ in range(self.CURRICULA_QUEUE.qsize())])  # type: ignore
        self.CURRICULA_DONE_EVENT.set()
        curricula: list[Curriculum2018] = self._flatten(nested_curricula)
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(curricula, iceberg_configuration)
        return curricula
