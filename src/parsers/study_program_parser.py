import asyncio
import logging
from pathlib import Path
from queue import Queue
from typing import List

from bs4 import Tag, BeautifulSoup

from src.models.named_tuples import StudyProgram
from src.configurations import StorageConfiguration, DatasetConfiguration
from src.parsers.base_parser import Parser


class StudyProgramParser(Parser):
    # https://finki.ukim.mk/mk/dodiplomski-studii
    STUDY_PROGRAMS_URL: str = 'https://finki.ukim.mk/mk/dodiplomski-studii'

    STUDY_PROGRAMS_2023_LI_SELECTOR: str = 'div > div > div > div > div > ul > li > div'
    STUDY_PROGRAM_URL_SELECTOR: str = 'a[href]'
    STUDY_PROGRAM_NAME_SELECTOR: str = 'span:nth-child(1)'
    STUDY_PROGRAM_DURATION_SELECTOR: str = 'span:nth-child(2)'

    STUDY_PROGRAMS_QUEUE: Queue = Queue()
    STUDY_PROGRAMS_READY_EVENT: asyncio.Event = asyncio.Event()

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> StudyProgram:
        study_program_row: Tag = kwargs.get('element')

        study_program: StudyProgram = StudyProgram(
            study_program_name=cls.extract_text(study_program_row, cls.STUDY_PROGRAM_NAME_SELECTOR),
            study_program_duration=int(cls.extract_text(study_program_row, cls.STUDY_PROGRAM_DURATION_SELECTOR)),
            study_program_url=cls.extract_url(study_program_row, cls.STUDY_PROGRAM_URL_SELECTOR)
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[StudyProgram]:
        def is_macedonian_study_program(study_program: StudyProgram) -> bool:
            return study_program.study_program_url.endswith('mk')

        soup: BeautifulSoup = kwargs.get('soup')
        study_program_elements: List[Tag] = soup.select(cls.STUDY_PROGRAMS_2023_LI_SELECTOR)

        study_programs: List[StudyProgram] = [await cls.parse_row(element=study_program) for study_program in study_program_elements]

        return list(filter(is_macedonian_study_program, study_programs))

    @classmethod
    async def run(cls) -> list[StudyProgram]:
        soup: BeautifulSoup = await Parser.get_parsed_html(cls.STUDY_PROGRAMS_URL)
        study_programs: List[StudyProgram] = await cls.parse_data(soup=soup)
        for study_program in study_programs:
            cls.STUDY_PROGRAMS_QUEUE.put_nowait(study_program)
            if not cls.STUDY_PROGRAMS_READY_EVENT.is_set():
                cls.STUDY_PROGRAMS_READY_EVENT.set()
        logging.info("Finished processing study programs")
        await cls.validate(study_programs, await cls.load_schema(DatasetConfiguration.STUDY_PROGRAMS))
        await cls.save_data(study_programs, DatasetConfiguration.STUDY_PROGRAMS)
        return study_programs
