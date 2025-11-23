import asyncio
import logging
import queue
import re
import threading
from abc import abstractmethod
from asyncio import Task
from concurrent.futures import Executor
from functools import partial
from ssl import SSLContext

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from src.models.types import Course
from src.network import HTTPClient
from src.parsers.base_curriculum_parser import BaseCurriculumParser
from src.parsers.base_parser import BaseParser


class BaseCourseParser(BaseParser):


    def __init__(self):
        self.queue: queue.Queue = queue.Queue()
        self.done_event: threading.Event = threading.Event()
        self.course_urls_set: set[str] = set()

    @property
    @abstractmethod
    def main_selector(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def code_selector(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def name_selector(self) -> str:
        raise NotImplementedError

    @property
    def file_selector(self) -> str:
        return 'div > div > div > span > a'

    @classmethod
    def code_regex(cls, accreditation_year: int) -> re.Pattern[str]:
        return re.compile(rf'F{accreditation_year % 100}L[1-3][SW]\d{{2,3}}')

    def parse_row(self, *args, **kwargs) -> Course:
        pass

    def parse_data(self, *args, **kwargs) -> list[Course] | Course:
        pass

    @classmethod
    def non_null(cls, courses: list[Course]) -> list[Course]:
        return list(filter(lambda x: x, courses))

    async def run(self,
                  session: ClientSession,
                  ssl_context: SSLContext,
                  http_client: HTTPClient,
                  curriculum_parser: BaseCurriculumParser,
                  semaphore: asyncio.Semaphore,
                  executor: Executor
                  ) -> list[Course]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, curriculum_parser.ready_event.wait)
        html_tasks: list[Task[tuple[int, str, str]]] = []
        pdf_tasks: list[Task[tuple[int, bytes, str, str, str]]] = []
        while True:
            try:
                course_url: str = curriculum_parser.course_urls_queue.get_nowait()
            except queue.Empty:
                if curriculum_parser.done_event.is_set():
                    break
                else:
                    await asyncio.sleep(0.1)
                    continue

            if course_url not in self.course_urls_set:
                html_tasks.append(asyncio.create_task(
                    http_client.fetch_page_limited(
                                                   strategy=http_client.text_strategy,
                                                   session=session,
                                                   ssl_context=ssl_context,
                                                   url=course_url,
                                                   semaphore=semaphore,
                                                   )
                ))
                self.course_urls_set.add(course_url)

        for task in asyncio.as_completed(html_tasks):
            http_status, page_content, html_url = await task
            soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
            pdf_url: str = self.extract_url(soup, self.file_selector, False)
            pdf_tasks.append(asyncio.create_task(
                http_client.fetch_page_limited(
                                               strategy=http_client.bytes_strategy,
                                               session=session,
                                               ssl_context=ssl_context,
                                               url=pdf_url,
                                               semaphore=semaphore,
                                               html_url=html_url,
                                               page_content=page_content,
                                               )
            ))


        for task in asyncio.as_completed(pdf_tasks):
            http_status, stream, pdf_url, html_url, page_content = await task
            self.queue.put_nowait(
                loop.run_in_executor(executor,
                                     partial(self.parse_data,
                                             http_client=http_client,
                                             session=session,
                                             ssl_context=ssl_context,
                                             semaphore=semaphore,
                                             url=html_url,
                                             stream=stream,
                                             page_content=page_content)))

        courses: list[Course] = await asyncio.gather(
            *[self.queue.get_nowait() for _ in range(self.queue.qsize())])  # type: ignore
        self.set_event(self.done_event)
        courses: list[Course] = self.non_null(courses)
        logging.info(f"Finished processing {Course.__name__}")
        return courses