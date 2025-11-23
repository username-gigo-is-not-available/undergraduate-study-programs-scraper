import asyncio
import logging
import queue
import threading
from abc import abstractmethod
from asyncio import Task
from ssl import SSLContext

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from src.models.types import StudyProgram
from src.network import HTTPClient
from src.parsers.base_parser import BaseParser


class BaseStudyProgramParser(BaseParser):
    STUDY_PROGRAM_MAX_DURATION: int = 4

    def __init__(self):
        self.ready_event: asyncio.Event = asyncio.Event()
        self.queue: queue.Queue = queue.Queue()
        self.done_event: threading.Event = threading.Event()
    @property
    @abstractmethod
    def li_selector(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def main_selector(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def name_selector(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def duration_selector(self) -> str:
        raise NotImplementedError

    @property
    def url_selector(self) -> str:
        return 'a[href]'

    @property
    @abstractmethod
    def title_selector(self) -> str:
        raise NotImplementedError

    def parse_row(self, *args, **kwargs) -> StudyProgram:
        pass

    def parse_data(self, *args, **kwargs) -> StudyProgram | list[StudyProgram]:
        pass

    @staticmethod
    def is_macedonian_study_program(url: str) -> bool:
        return '/mk' in url

    def extract_study_program_urls(self, page_content: str) -> list[str]:
        soup: BeautifulSoup = self.get_parsed_html(page_content)
        return [self.extract_url(element, self.url_selector) for element in soup.select(self.li_selector)]

    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  page_content: str,
                  http_client: HTTPClient,
                  semaphore: asyncio.Semaphore) -> list[StudyProgram]:

        tasks: list[Task[tuple[int, str, str]]] = []
        study_programs: list[StudyProgram] = []

        study_program_urls: list[str] = list(filter(self.is_macedonian_study_program, self.extract_study_program_urls(page_content)))
        for study_program_url in study_program_urls:
            tasks.append(
                asyncio.create_task(
                    http_client.fetch_page_limited(
                        strategy=http_client.text_strategy,
                        session=session,
                        ssl_context=ssl_context,
                        url=study_program_url,
                        semaphore=semaphore
                    )
                )
            )

        for task in asyncio.as_completed(tasks):
            http_status, page_content, url = await task
            for study_program in self.parse_data(page_content=page_content, url=url):
                self.queue.put_nowait((study_program, page_content))
                study_programs.append(study_program)
                self.set_event(self.ready_event)

        self.set_event(self.done_event)
        logging.info(f"Finished processing {StudyProgram.__name__}")
        return study_programs