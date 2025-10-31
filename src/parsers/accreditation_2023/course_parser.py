import asyncio
import logging
import queue
import threading
from asyncio import Task
from concurrent.futures import Executor
from functools import partial
from queue import Queue
from ssl import SSLContext

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import TableConfiguration
from src.corrector import CourseCorrector
from src.models.data_classes import Course
from src.network import HTTPClient
from src.parsers.accreditation_2023.curriculum_parser import CurriculumParser
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class CourseParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSE_TABLE_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
    COURSE_NAME_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
    COURSE_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3) > p > span:nth-child(even)'
    COURSE_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3) > p > span'
    COURSE_COMPETENCE_SELECTOR: str =  'tr:nth-child(9) > td:nth-child(2) > p:nth-child(3) > span'
    COURSE_CONTENT_SELECTOR: str =  'tr:nth-child(10) > td:nth-child(2) > p:nth-child(3) > span'
    COURSES_QUEUE: Queue = Queue()
    COURSES_DONE_EVENT: threading.Event = threading.Event()
    PROCESSED_COURSE_URLS: set[str] = set()

    def parse_row(self, *args, **kwargs) -> Course:
        url: str = kwargs.get('url')
        element: Tag = kwargs.get('element')

        fields: dict[str, str | int] = CourseCorrector.correct({
            'code': self.extract_text(element, self.COURSE_CODE_SELECTOR),
            'name': self.extract_text(element, self.COURSE_NAME_SELECTOR),
        })

        course: Course = Course(
            code=fields.get('code'),
            name=fields.get('name'),
            url=url,
            professors=", ".join(self.extract_multiple_texts(element, self.COURSE_PROFESSORS_SELECTOR)),
            prerequisites=self.extract_text(element, self.COURSE_PREREQUISITE_SELECTOR),
            competence=self.extract_text(element, self.COURSE_COMPETENCE_SELECTOR),
            content=self.extract_text(element, self.COURSE_CONTENT_SELECTOR),
        )
        logging.info(f"Scraped course {course}")
        return course

    def parse_data(self, *args, **kwargs) -> Course:
        url = kwargs.get('url')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = Parser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.COURSE_TABLE_CLASS_NAME)
        return self.parse_row(url=url, element=element)

    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  iceberg_configuration: TableConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  semaphore: asyncio.Semaphore | None = None,
                  executor: Executor | None = None) -> list[Course]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, CurriculumParser.COURSE_HEADERS_READY_EVENT.wait)
        tasks: list[Task[tuple[int, str, str]]] = []
        while True:
            try:
                course_url: str = CurriculumParser.COURSE_URLS_QUEUE.get_nowait()
            except queue.Empty:
                if CurriculumParser.CURRICULA_DONE_EVENT.is_set():
                    break
                else:
                    await asyncio.sleep(0.1)
                    continue

            if course_url not in self.PROCESSED_COURSE_URLS:
                tasks.append(asyncio.create_task(
                    http_client.fetch_page_limited(session=session,
                                                   ssl_context=ssl_context,
                                                   url=course_url,
                                                   semaphore=semaphore,
                                                   )
                ))
                self.PROCESSED_COURSE_URLS.add(course_url)

        for task in asyncio.as_completed(tasks):
            http_status, page_content, course_url = await task
            self.COURSES_QUEUE.put_nowait(
            loop.run_in_executor(executor, partial(self.parse_data, url=course_url, page_content=page_content)))

        courses: list[Course] = await asyncio.gather(
            *[self.COURSES_QUEUE.get_nowait() for _ in range(self.COURSES_QUEUE.qsize())])  # type: ignore
        self.COURSES_DONE_EVENT.set()
        logging.info(f"Finished processing {iceberg_configuration}")
        await iceberg_client.save_data(courses, iceberg_configuration)
        return courses