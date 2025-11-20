import asyncio
import logging
import queue
import threading
from abc import abstractmethod
from concurrent.futures import Executor
from functools import reduce, partial

from bs4 import Tag
from pyiceberg.schema import Schema

from src.models.enums import OfferingType
from src.models.types import Curriculum, StudyProgram
from src.parsers.base_parser import BaseParser
from src.parsers.base_study_program_parser import BaseStudyProgramParser
from src.storage import IcebergClient


class BaseCurriculumParser(BaseParser):

    def __init__(self):
        self.queue: queue.Queue = queue.Queue()
        self.course_urls_queue: queue.Queue = queue.Queue()
        self.done_event: asyncio.Event = asyncio.Event()
        self.ready_event: threading.Event = threading.Event()

    @property
    def course_table_rows_selector(self) -> str:
        return 'tr'

    @property
    @abstractmethod
    def course_url_selector(self) -> str:
        raise NotImplementedError

    def parse_row(self, *args, **kwargs) -> Curriculum:
        pass

    def parse_rows(self,
                        rows: list[Tag],
                        offering_type: OfferingType,
                        semester: int,
                        study_program: StudyProgram
                           ):
        return [self.parse_row(
                        element=row,
                        offering_type=offering_type,
                        semester=semester,
                        study_program=study_program
                ) for row in rows]

    def parse_data(self, *args, **kwargs) -> list[Curriculum]:
        pass

    @classmethod
    def is_valid_course_row(cls, row: Tag, selector: str) -> bool:
        return bool(row.select_one(selector))

    @classmethod
    def flatten(cls, data: list[list[Curriculum]]) -> list[Curriculum]:
        return reduce(lambda x, y: x + y, data)

    def extract_course_rows_from_section(self, section: Tag) -> list[Tag]:
        return [row for row in section.select(self.course_table_rows_selector) if self.is_valid_course_row(row, self.course_url_selector)]

    async def run(self,
                  table_name: str,
                  schema: Schema,
                  iceberg_client: IcebergClient,
                  study_program_parser: BaseStudyProgramParser,
                  executor: Executor) -> list[Curriculum]:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        await study_program_parser.ready_event.wait()
        while True:
            try:
                study_program, page_content = study_program_parser.queue.get_nowait()
            except queue.Empty:
                if study_program_parser.done_event.is_set():
                    break
                else:
                    await asyncio.sleep(0.1)
                    continue

            self.queue.put_nowait(loop.run_in_executor(executor, partial(self.parse_data,
                                                                                   study_program=study_program,
                                                                                   page_content=page_content)))

        nested_curricula: list[list[Curriculum]] = await asyncio.gather(
            *[self.queue.get_nowait() for _ in range(self.queue.qsize())])  # type: ignore

        self.set_event(self.done_event)
        curricula: list[Curriculum] = self.flatten(nested_curricula)
        logging.info(f"Finished processing {table_name}")
        await iceberg_client.save_data(curricula, table_name, schema)
        return curricula
