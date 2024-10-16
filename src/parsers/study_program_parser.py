import asyncio
import logging
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from queue import Queue
from typing import List

from bs4 import Tag, BeautifulSoup

from src.parsers.base_parser import Parser
from src.parsers.field_parser import FieldParser
from src.models import StudyProgram
from src.static import ENVIRONMENT_VARIABLES


class StudyProgramParser(Parser):
    # https://finki.ukim.mk/mk/dodiplomski-studii
    STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'))
    STUDY_PROGRAMS_URL: str = 'https://finki.ukim.mk/mk/dodiplomski-studii'
    STUDY_PROGRAMS_2023_LI_SELECTOR: str = 'div > div > div > div > div > ul > li > div'
    STUDY_PROGRAM_URL_SELECTOR: str = 'a[href]'
    STUDY_PROGRAM_NAME_SELECTOR: str = 'span:nth-child(1)'
    STUDY_PROGRAM_DURATION_SELECTOR: str = 'span:nth-child(2)'
    STUDY_PROGRAMS_QUEUE: Queue = Queue()
    STUDY_PROGRAMS_DONE_EVENT: asyncio.Event = asyncio.Event()

    @classmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        return [
            FieldParser('study_program_name', cls.STUDY_PROGRAM_NAME_SELECTOR, element, FieldParser.parse_text_field),
            FieldParser('study_program_duration', cls.STUDY_PROGRAM_DURATION_SELECTOR, element, partial(FieldParser.parse_text_field,
                                                                                                        field_type=int)),
            FieldParser('study_program_url', cls.STUDY_PROGRAM_URL_SELECTOR, element, FieldParser.parse_url_field),
        ]

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> StudyProgram:
        study_program_row: Tag = kwargs.get('element')
        study_program: StudyProgram = StudyProgram(**cls.parse_fields(element=study_program_row))
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[StudyProgram]:
        def is_macedonian_study_program(study_program: StudyProgram) -> bool:
            return study_program.study_program_url.endswith('mk')

        soup: BeautifulSoup = await Parser.get_parsed_html(cls.STUDY_PROGRAMS_URL)
        study_program_elements: List[Tag] = soup.select(cls.STUDY_PROGRAMS_2023_LI_SELECTOR)

        tasks: list[asyncio.Task[StudyProgram]] = [
            asyncio.create_task(cls.parse_row(element=element))
            for element in study_program_elements
        ]

        study_programs: list[StudyProgram] = [_ for _ in await asyncio.gather(*tasks)]

        return list(filter(is_macedonian_study_program, study_programs))

    @classmethod
    async def scrape_and_save_data(cls,
                                   executor: Executor = None,
                                   *args,
                                   **kwargs
                                   ) -> list[StudyProgram]:
        data: list[StudyProgram] = await cls.scrape_data(
            output_event=cls.STUDY_PROGRAMS_DONE_EVENT,
            output_queue=cls.STUDY_PROGRAMS_QUEUE,
            parse_func=cls.parse_data,
            done_log_msg="Finished scraping study programs",
            *args,
            **kwargs)
        await cls.save_data(executor=executor, data=data, file_name=cls.STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME)
        return data
