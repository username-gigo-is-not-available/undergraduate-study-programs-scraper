import asyncio
import logging
import threading
from asyncio import Event, Task
from concurrent.futures import Executor
from functools import partial, reduce
from pathlib import Path
from queue import Queue

from bs4 import Tag, BeautifulSoup

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.models.enums import CourseType
from src.models.named_tuples import CurriculumHeader, StudyProgram, CourseHeader
from src.parsers.base_parser import Parser
from src.parsers.study_program_parser import StudyProgramParser
from src.patterns.strategy.corrector import CourseCorrectorStrategy


class CurriculumParser(Parser):
    # https://finki.ukim.mk/program/{program_name}

    MANDATORY_COURSE_SECTION_SELECTOR: str = '.col-md-6.col-sm-12'
    ELECTIVE_COURSE_SECTION_SELECTOR: str = '.col-md-12.col-sm-12'
    COURSE_SECTION_ROWS_SELECTOR: str = 'tr'
    COURSE_CODE_SELECTOR: str = 'td:nth-child(1)'
    COURSE_NAME_AND_URL_SELECTOR: str = 'td:nth-child(2) > a'
    COURSE_SEMESTER_SELECTOR: str = 'td:nth-child(3)'
    MANDATORY_COURSE_SEMESTER_SELECTOR: str = 'h3 > span'

    COURSE_HEADERS_QUEUE: Queue = Queue()
    COURSE_HEADERS_READY_EVENT: Event = Event()
    CURRICULA_QUEUE: Queue = Queue()
    CURRICULA_DONE_EVENT: Event = Event()
    STUDY_PROGRAMS_LOCK: threading.Lock = threading.Lock()

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> CurriculumHeader:

        study_program: StudyProgram = kwargs.get('study_program')
        course_row: Tag = kwargs.get('element')
        course_type: CourseType = kwargs.get('course_type')

        fields: dict[str, str | int] = CourseCorrectorStrategy.correct({
            'course_code': cls.extract_text(course_row, cls.COURSE_CODE_SELECTOR),
            'course_name_mk': cls.extract_text(course_row, cls.COURSE_NAME_AND_URL_SELECTOR),
            'course_url': cls.extract_url(course_row, cls.COURSE_NAME_AND_URL_SELECTOR),
            'course_type': course_type,
            'course_semester': int(cls.extract_text(course_row, cls.COURSE_SEMESTER_SELECTOR))
        })
        course_header: CourseHeader = CourseHeader(
            course_code=fields.get('course_code'),
            course_name_mk=fields.get('course_name_mk'),
            course_url=fields.get('course_url'),
        )

        cls.COURSE_HEADERS_QUEUE.put_nowait(course_header)
        cls.COURSE_HEADERS_READY_EVENT.set()

        curriculum: CurriculumHeader = CurriculumHeader(**{**study_program._asdict(), **fields})
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[CurriculumHeader]:

        async def parse_section(study_program: StudyProgram, course_type: CourseType, section: Tag) -> list[CurriculumHeader]:
            def filter_course_rows(row: Tag) -> bool:
                return bool(row.select_one(cls.COURSE_CODE_SELECTOR) and row.select_one(cls.COURSE_NAME_AND_URL_SELECTOR))

            def modify_course_row(row: Tag, tag_name: str, text: str) -> Tag:
                tag: Tag = Tag(name=tag_name)
                tag.string = text
                row.append(tag)
                return row

            filtered_course_rows: list[Tag] = list(filter(filter_course_rows, section.select(cls.COURSE_SECTION_ROWS_SELECTOR)))
            augmented_course_rows: list[Tag] = [modify_course_row(row, 'td', cls.extract_text(section, cls.MANDATORY_COURSE_SEMESTER_SELECTOR))
                                      if course_type == CourseType.MANDATORY else row for row in filtered_course_rows]

            tasks: list[Task[CurriculumHeader]] = [asyncio.create_task(
                cls.parse_row(study_program=study_program, element=course_row, course_type=course_type))
                for course_row in augmented_course_rows]

            return await asyncio.gather(*tasks)  # type: ignore

        study_program: StudyProgram = kwargs.get('item')
        soup: BeautifulSoup = await cls.get_parsed_html(study_program.study_program_url)
        tasks: list[Task[list[CurriculumHeader]]] = []

        for section in soup.select(cls.MANDATORY_COURSE_SECTION_SELECTOR):
            tasks.append(asyncio.create_task(parse_section(study_program, CourseType.MANDATORY, section)))
        for section in soup.select(cls.ELECTIVE_COURSE_SECTION_SELECTOR):
            tasks.append(asyncio.create_task(parse_section(study_program, CourseType.ELECTIVE, section)))

        return reduce(lambda x, y: x + y, await asyncio.gather(*tasks))  # type: ignore

    @classmethod
    async def run(cls, executor: Executor) -> list[CurriculumHeader]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

        await StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.wait()

        while True:
            async with cls.async_lock(cls.STUDY_PROGRAMS_LOCK, executor):
                if StudyProgramParser.STUDY_PROGRAMS_READY_EVENT.is_set() and StudyProgramParser.STUDY_PROGRAMS_QUEUE.empty():
                    cls.CURRICULA_DONE_EVENT.set()
                    nested_curricula: list[list[CurriculumHeader]] = await asyncio.gather(
                        *[cls.CURRICULA_QUEUE.get_nowait() for _ in range(cls.CURRICULA_QUEUE.qsize())])  # type: ignore
                    curricula: list[CurriculumHeader] = reduce(lambda x, y: x + y, nested_curricula)
                    logging.info("Finished processing curricula")
                    await cls.validate(curricula, await cls.load_schema(DatasetConfiguration.CURRICULA))
                    await cls.save_data(curricula, DatasetConfiguration.CURRICULA)
                    return curricula

            while not StudyProgramParser.STUDY_PROGRAMS_QUEUE.empty():
                study_program: StudyProgram = StudyProgramParser.STUDY_PROGRAMS_QUEUE.get_nowait()
                cls.CURRICULA_QUEUE.put_nowait(loop.run_in_executor(executor, partial(cls.run_parse_data, item=study_program)))
