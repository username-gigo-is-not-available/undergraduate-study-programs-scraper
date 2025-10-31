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
from src.models.data_classes import StudyProgram
from src.network import HTTPClient
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class StudyProgramParser(Parser):
    # https://finki.ukim.mk/mk/dodiplomski-studii

    STUDY_PROGRAMS_2023_LI_SELECTOR: str = '#block-views-akreditacija-2023-block-1 > div > div > div > div > div > ul > li > div'
    STUDY_PROGRAM_DIV_SELECTOR: str = '#block-system-main > div > div > div > div:nth-child(7)'
    STUDY_PROGRAM_URL_SELECTOR: str = 'a[href]'
    STUDY_PROGRAM_NAME_SELECTOR: str = 'h2 > span:nth-child(1)'
    STUDY_PROGRAM_DURATION_SELECTOR: str = 'h2 > span:nth-child(2)'
    STUDY_PROGRAM_TITLE_SELECTOR: str = 'div:nth-child(3) > div:nth-child(2) > h5'
    STUDY_PROGRAMS_QUEUE: queue.Queue = queue.Queue()
    STUDY_PROGRAMS_READY_EVENT: asyncio.Event = asyncio.Event()
    STUDY_PROGRAMS_DONE_EVENT: threading.Event = threading.Event()

    def parse_row(self, *args, **kwargs) -> StudyProgram:
        element: Tag = kwargs.get('element')
        url: str = kwargs.get('url')

        study_program: StudyProgram = StudyProgram(
            name=self.extract_text(element, self.STUDY_PROGRAM_NAME_SELECTOR),
            duration=int(self.extract_text(element, self.STUDY_PROGRAM_DURATION_SELECTOR)),
            url=url,
            title=self.extract_text(element, self.STUDY_PROGRAM_TITLE_SELECTOR)
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    @classmethod
    def _is_macedonian_study_program(cls, url: str) -> bool:
        return url.endswith('mk')

    @classmethod
    def _get_study_program_urls(cls, page_content: str):
        soup: BeautifulSoup = cls.get_parsed_html(page_content)
        return [cls.extract_url(element, cls.STUDY_PROGRAM_URL_SELECTOR) for element in soup.select(cls.STUDY_PROGRAMS_2023_LI_SELECTOR)]

    def parse_data(self, *args, **kwargs) -> StudyProgram:
        page_content: str = kwargs.get('page_content')
        url: str = kwargs.get('url')
        soup: BeautifulSoup = Parser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.STUDY_PROGRAM_DIV_SELECTOR)
        return self.parse_row(element=element, url=url)

    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[StudyProgram]:

        tasks: list[Task[tuple[int, str, str]]] = []
        study_programs: list[StudyProgram] = []
        http_status, page_content, url = await http_client.fetch_page(
            session=session,
            ssl_context=ssl_context,
            url=ApplicationConfiguration.STUDY_PROGRAMS_URL
        )

        study_program_urls: list[str] = list(filter(self._is_macedonian_study_program, self._get_study_program_urls(page_content)))
        for study_program_url in study_program_urls:
            tasks.append(
                asyncio.create_task(
                    http_client.fetch_page_limited(
                        session=session,
                        ssl_context=ssl_context,
                        url=study_program_url,
                        semaphore=semaphore
                    )
                )
            )

        for task in asyncio.as_completed(tasks):
            http_status, page_content, url = await task
            study_program: StudyProgram = self.parse_data(page_content=page_content, url=url)
            self.STUDY_PROGRAMS_QUEUE.put_nowait((study_program, page_content))
            study_programs.append(study_program)
            if not self.STUDY_PROGRAMS_READY_EVENT.is_set():
                self.STUDY_PROGRAMS_READY_EVENT.set()

        self.STUDY_PROGRAMS_DONE_EVENT.set()
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(study_programs, iceberg_configuration)
        return study_programs
