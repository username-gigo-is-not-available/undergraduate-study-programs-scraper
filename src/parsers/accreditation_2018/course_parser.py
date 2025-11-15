import asyncio
import io
import logging
import queue
import re
import threading
from asyncio import Task
from concurrent.futures import Executor
from functools import partial
from io import BytesIO
from queue import Queue
from ssl import SSLContext
import pypdfium2
import fitz
from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import TableConfiguration, ApplicationConfiguration
from src.models.accreditation_2018.data_classes import Course2018
from src.network import HTTPClient
from src.parsers.accreditation_2018.curriculum_parser import CurriculumParser
from src.parsers.base_parser import Parser
from src.storage import IcebergClient
class CourseParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSE_ACCREDITATION_YEAR: int = 2018
    COURSE_SECTION_SELECTOR: str = '.row > section'
    COURSE_CODE_SELECTOR: str = 'div > div > div'
    COURSE_NAME_SELECTOR: str = 'div > ul > li'
    COURSE_FILE_SELECTOR: str = 'div > div > div > span > a'
    COURSES_QUEUE: Queue = Queue()
    COURSES_DONE_EVENT: threading.Event = threading.Event()
    COURSE_URLS_SET: set[str] = set()

    def parse_row(self, *args, **kwargs) -> Course2018 | None:
        url: str = kwargs.get('url')
        stream: bytes = kwargs.get('stream')
        course_section: Tag = kwargs.get('element')
        code: str = self.extract_text(course_section, self.COURSE_CODE_SELECTOR)

        if not re.search(ApplicationConfiguration.COURSE_2018_CODE_REGEX, code) and code:
            logging.info(f"Failed to process course {url}")
            return None

        document: fitz.Document | None = fitz.open(stream=io.BytesIO(stream)) if stream else None
        if not document:
            text: str = ""
        else:
            text: str = "".join([page.get_text() for page in document])
        course: Course2018 = Course2018(
            accreditation_year=self.COURSE_ACCREDITATION_YEAR,
            code=code,
            name=self.extract_text(course_section, self.COURSE_NAME_SELECTOR),
            url=url,
            text=text,
        )
        logging.info(f"Scraped course {course}")
        return course

    def parse_data(self, *args, **kwargs) -> Course2018:
        url: str = kwargs.get('url')
        stream: bytes = kwargs.get('stream')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = Parser.get_parsed_html(page_content)
        course_section: Tag = soup.select_one(self.COURSE_SECTION_SELECTOR)
        return self.parse_row(element=course_section, url=url, stream=stream)


    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Course2018]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, CurriculumParser.COURSE_URLS_READY_EVENT.wait)
        html_tasks: list[Task[tuple[int, str, str]]] = []
        pdf_tasks: list[Task[tuple[int, bytes, str, str, str]]] = []
        while True:
            try:
                course_url: str = CurriculumParser.COURSE_URLS_QUEUE.get_nowait()
            except queue.Empty:
                if CurriculumParser.CURRICULA_DONE_EVENT.is_set():
                    break
                else:
                    await asyncio.sleep(0.1)
                    continue

            if course_url not in self.COURSE_URLS_SET:
                html_tasks.append(asyncio.create_task(
                    http_client.fetch_text_limited(session=session,
                                                   ssl_context=ssl_context,
                                                   url=course_url,
                                                   semaphore=semaphore,
                                                   )
                ))
                self.COURSE_URLS_SET.add(course_url)

        for task in asyncio.as_completed(html_tasks):
            http_status, page_content, html_url = await task
            soup: BeautifulSoup = Parser.get_parsed_html(page_content)
            pdf_url: str | None = self.extract_url(soup, self.COURSE_FILE_SELECTOR, False)
            if pdf_url:
                pdf_tasks.append(asyncio.create_task(
                    http_client.fetch_bytes_limited(session=session,
                                                   ssl_context=ssl_context,
                                                   url=pdf_url,
                                                   semaphore=semaphore,
                                                   html_url=html_url,
                                                   page_content=page_content,
                                                   )
                ))
            else:
                self.COURSES_QUEUE.put_nowait(
                    loop.run_in_executor(executor,
                                         partial(self.parse_data,
                                                 http_client=http_client,
                                                 session=session,
                                                 ssl_context=ssl_context,
                                                 semaphore=semaphore,
                                                 url=html_url,
                                                 stream=b'',
                                                 page_content=page_content)))

        for task in asyncio.as_completed(pdf_tasks):
            http_status, stream, pdf_url, html_url, page_content = await task
            self.COURSES_QUEUE.put_nowait(
                loop.run_in_executor(executor,
                                     partial(self.parse_data,
                                             http_client=http_client,
                                             session=session,
                                             ssl_context=ssl_context,
                                             semaphore=semaphore,
                                             url=html_url,
                                             stream=stream,
                                             page_content=page_content)))

        courses: list[Course2018] = await asyncio.gather(
            *[self.COURSES_QUEUE.get_nowait() for _ in range(self.COURSES_QUEUE.qsize())])  # type: ignore
        self.COURSES_DONE_EVENT.set()
        courses: list[Course2018] = list(filter(lambda x: x, courses))
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(courses, iceberg_configuration)
        return courses