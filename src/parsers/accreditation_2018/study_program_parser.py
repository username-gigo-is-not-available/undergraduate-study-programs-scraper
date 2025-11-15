import asyncio
import logging
import queue
import threading
from asyncio import Task
from concurrent.futures import Executor
from ssl import SSLContext

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import ApplicationConfiguration, TableConfiguration
from src.models.accreditation_2018.data_classes import StudyProgram2018
from src.network import HTTPClient
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class StudyProgramParser(Parser):
    # https://finki.ukim.mk/mk/dodiplomski-studii

    STUDY_PROGRAM_ACCREDITATION_YEAR: int = 2018
    STUDY_PROGRAMS_LI_SELECTOR: str = '#block-system-main > div > div > div > ul > li > div'
    STUDY_PROGRAM_SECTION_SELECTOR: str = 'body > div> div > div > section'
    STUDY_PROGRAM_URL_SELECTOR: str = 'a[href]'
    STUDY_PROGRAM_NAME_SELECTOR: str = 'div > ul > li'
    STUDY_PROGRAM_DURATION_SELECTOR_TEMPLATE: str = 'div > div > div > p:nth-child(5) > strong:nth-child({parity})'
    STUDY_PROGRAM_DURATION_SELECTOR: str = 'div > div > div > p:nth-child(5) > strong:nth-child(odd)'
    STUDY_PROGRAM_TITLE_SELECTOR: str = 'div > div > div > p:nth-child(5)'
    STUDY_PROGRAM_NUMBER_OF_DURATIONS_SELECTOR: str = 'div > div > div > p:nth-child(5) > strong'
    STUDY_PROGRAMS_QUEUE: queue.Queue = queue.Queue()
    STUDY_PROGRAMS_READY_EVENT: asyncio.Event = asyncio.Event()
    STUDY_PROGRAMS_DONE_EVENT: threading.Event = threading.Event()

    def parse_row(self, *args, **kwargs) -> StudyProgram2018:
        element: Tag = kwargs.get('element')
        url: str = kwargs.get('url')
        name: str = self.extract_text(element, self.STUDY_PROGRAM_NAME_SELECTOR)
        duration: str = self.extract_text(element, self.STUDY_PROGRAM_DURATION_SELECTOR)
        study_program: StudyProgram2018 = StudyProgram2018(
            accreditation_year=self.STUDY_PROGRAM_ACCREDITATION_YEAR,
            name=name,
            duration=duration,
            url=url,
            title=self.extract_text(element, self.STUDY_PROGRAM_TITLE_SELECTOR),
            full_name=f"{name}_{duration}",
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    @classmethod
    def _is_macedonian_study_program(cls, url: str) -> bool:
        return '/mk/' in url

    @classmethod
    def _get_study_program_urls(cls, page_content: str):
        soup: BeautifulSoup = cls.get_parsed_html(page_content)
        return [cls.extract_url(element, cls.STUDY_PROGRAM_URL_SELECTOR) for element in soup.select(cls.STUDY_PROGRAMS_LI_SELECTOR)]


    def parse_data(self, *args, **kwargs) -> list[StudyProgram2018]:
        page_content: str = kwargs.get('page_content')
        url: str = kwargs.get('url')
        study_programs: list[StudyProgram2018] = []
        soup: BeautifulSoup = Parser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.STUDY_PROGRAM_SECTION_SELECTOR)
        number_of_titles: int = len(element.select(self.STUDY_PROGRAM_NUMBER_OF_DURATIONS_SELECTOR))
        for duration in range(1, number_of_titles+1):
            parity: str = 'odd' if duration % 2 or number_of_titles == 1 else 'even'
            self.STUDY_PROGRAM_DURATION_SELECTOR = self.STUDY_PROGRAM_DURATION_SELECTOR_TEMPLATE.format(parity=parity)
            study_programs.append(self.parse_row(element=element, url=url))
        return study_programs

    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[StudyProgram2018]:

        tasks: list[Task[tuple[int, str, str]]] = []
        study_programs: list[StudyProgram2018] = []
        http_status, page_content, url = await http_client.fetch_text(
            session=session,
            ssl_context=ssl_context,
            url=ApplicationConfiguration.STUDY_PROGRAMS_URL
        )

        study_program_urls: list[str] = list(filter(self._is_macedonian_study_program, self._get_study_program_urls(page_content)))
        for study_program_url in study_program_urls:
            tasks.append(
                asyncio.create_task(
                    http_client.fetch_text_limited(
                        session=session,
                        ssl_context=ssl_context,
                        url=study_program_url,
                        semaphore=semaphore,
                    )
                )
            )

        for task in asyncio.as_completed(tasks):
            http_status, page_content, url = await task
            for study_program in self.parse_data(page_content=page_content, url=url):
                self.STUDY_PROGRAMS_QUEUE.put_nowait((study_program, page_content))
                study_programs.append(study_program)
                if not self.STUDY_PROGRAMS_READY_EVENT.is_set():
                    self.STUDY_PROGRAMS_READY_EVENT.set()

        self.STUDY_PROGRAMS_DONE_EVENT.set()
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(study_programs, iceberg_configuration)
        return study_programs
