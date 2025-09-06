import asyncio
import logging
import threading
from asyncio import Task
from concurrent.futures import Executor
from functools import partial
from queue import Queue
from ssl import SSLContext

import aiohttp
from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import DatasetConfiguration
from src.models.named_tuples import CourseDetails, CourseHeader
from src.parsers.base_parser import Parser
from src.parsers.curriculum_parser import CurriculumParser
from src.patterns.strategy.corrector import CourseCorrectorStrategy


class CourseParser(Parser):
    # https://finki.ukim.mk/subject/{course_code}

    COURSE_TABLE_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
    COURSE_NAME_MK_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
    COURSE_NAME_EN_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(2) > span'
    COURSE_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
    COURSE_URL_SELECTOR: str = 'head > link:nth-child(7)'
    COURSE_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3)'
    COURSE_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3)'
    COURSE_ACADEMIC_YEAR_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
    COURSE_SEMESTER_SEASON_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
    COURSE_COMPETENCE_SELECTOR: str =  'tr:nth-child(9) > td:nth-child(2) > p:nth-child(3)'
    COURSE_CONTENT_SELECTOR: str =  'tr:nth-child(10) > td:nth-child(2) > p:nth-child(3)'
    COURSES_QUEUE: Queue = Queue()
    COURSES_DONE_EVENT: threading.Event = threading.Event()
    PROCESSED_COURSE_HEADERS: set[CourseHeader] = set()

    def parse_row(self, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('course_header')
        course_table: Tag = kwargs.get('element')

        fields: dict[str, str] = CourseCorrectorStrategy.correct({
            'course_name_en': self.extract_text(course_table, self.COURSE_NAME_EN_SELECTOR),
            'course_professors': self.extract_text(course_table, self.COURSE_PROFESSORS_SELECTOR),
            'course_prerequisites': self.extract_text(course_table, self.COURSE_PREREQUISITE_SELECTOR),
            'course_competence': self.extract_text(course_table, self.COURSE_COMPETENCE_SELECTOR),
            'course_content': self.extract_text(course_table, self.COURSE_CONTENT_SELECTOR),
        })

        course: CourseDetails = CourseDetails(**{**course_header._asdict(), **fields})
        logging.info(f"Scraped course {course}")
        return course

    def parse_data(self, *args, **kwargs) -> CourseDetails:
        course_header: CourseHeader = kwargs.get('course_header')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = Parser.get_parsed_html(page_content)
        course_table: Tag = soup.select_one(self.COURSE_TABLE_CLASS_NAME)
        return self.parse_row(course_header=course_header, element=course_table)

    async def fetch_page_wrapper(self, session: aiohttp.ClientSession, ssl_context: SSLContext, course_header: CourseHeader) -> tuple[str, CourseHeader]:
        return await self.fetch_page(session=session, ssl_context=ssl_context, url=course_header.course_url), course_header

    async def run(self, session: ClientSession, ssl_context: SSLContext, executor: Executor)  -> list[CourseDetails]:

        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await loop.run_in_executor(None, CurriculumParser.CURRICULA_DONE_EVENT.wait)
        tasks: list[Task[tuple[str, CourseHeader]]] = []
        while not CurriculumParser.COURSE_HEADERS_QUEUE.empty():
            course_header: CourseHeader = CurriculumParser.COURSE_HEADERS_QUEUE.get_nowait()

            if course_header not in self.PROCESSED_COURSE_HEADERS:
                tasks.append(asyncio.create_task(
                    self.fetch_page_wrapper(session=session,
                                            ssl_context=ssl_context,
                                            course_header=course_header,
                                            )
                ))
                self.PROCESSED_COURSE_HEADERS.add(course_header)

        results: list[dict[str, str | CourseHeader]] = await asyncio.gather(*tasks)
        for page_content, course_header in results:
            self.COURSES_QUEUE.put_nowait(
            loop.run_in_executor(executor, partial(self.parse_data, course_header=course_header, page_content=page_content)))

        self.COURSES_DONE_EVENT.set()
        courses: list[CourseDetails] = await asyncio.gather(
            *[self.COURSES_QUEUE.get_nowait() for _ in range(self.COURSES_QUEUE.qsize())])  # type: ignore
        logging.info(f"Finished processing {DatasetConfiguration.COURSES.dataset_name}")
        await self.validate(courses, await self.load_schema(DatasetConfiguration.COURSES))
        await self.save_data(courses, DatasetConfiguration.COURSES)
        return courses