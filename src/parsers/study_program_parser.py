import asyncio
import logging
import queue
from concurrent.futures import Executor
from http import HTTPStatus
from ssl import SSLContext
from typing import List, NamedTuple

from aiohttp import ClientSession
from bs4 import Tag, BeautifulSoup

from src.configurations import DatasetConfiguration, ApplicationConfiguration
from src.models.named_tuples import StudyProgram
from src.network import HTTPClient
from src.parsers.base_parser import Parser
from src.storage import IcebergClient


class StudyProgramParser(Parser):
    # https://finki.ukim.mk/mk/dodiplomski-studii

    STUDY_PROGRAMS_2023_LI_SELECTOR: str = 'div > div > div > div > div > ul > li > div'
    STUDY_PROGRAM_URL_SELECTOR: str = 'a[href]'
    STUDY_PROGRAM_NAME_SELECTOR: str = 'span:nth-child(1)'
    STUDY_PROGRAM_DURATION_SELECTOR: str = 'span:nth-child(2)'

    STUDY_PROGRAMS_QUEUE: queue.Queue = queue.Queue()
    STUDY_PROGRAMS_READY_EVENT: asyncio.Event = asyncio.Event()

    def parse_row(self, *args, **kwargs) -> StudyProgram:
        study_program_row: Tag = kwargs.get('element')

        study_program: StudyProgram = StudyProgram(
            study_program_name=self.extract_text(study_program_row, self.STUDY_PROGRAM_NAME_SELECTOR),
            study_program_duration=int(self.extract_text(study_program_row, self.STUDY_PROGRAM_DURATION_SELECTOR)),
            study_program_url=self.extract_url(study_program_row, self.STUDY_PROGRAM_URL_SELECTOR)
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    def parse_data(self, *args, **kwargs) -> list[StudyProgram]:
        def is_macedonian_study_program(study_program: StudyProgram) -> bool:
            return study_program.study_program_url.endswith('mk')

        soup: BeautifulSoup = kwargs.get('soup')
        study_program_elements: List[Tag] = soup.select(self.STUDY_PROGRAMS_2023_LI_SELECTOR)

        study_programs: List[StudyProgram] = [self.parse_row(element=study_program) for study_program in study_program_elements]

        return list(filter(is_macedonian_study_program, study_programs))

    async def run(self, session: ClientSession,
                  ssl_context: SSLContext,
                  dataset_configuration: DatasetConfiguration,
                  http_client: HTTPClient,
                  iceberg_client: IcebergClient,
                  executor: Executor | None = None) -> list[NamedTuple]:

        http_status, page_content = await http_client.fetch_page(session=session,
                                              ssl_context=ssl_context,
                                              url=ApplicationConfiguration.STUDY_PROGRAMS_URL,
                                              )
        if http_status != HTTPStatus.OK:
            logging.error(
                f"Tried to fetch {ApplicationConfiguration.STUDY_PROGRAMS_URL} but got HTTP status: {http_status}"
            )
            return []
        soup: BeautifulSoup = self.get_parsed_html(page_content)
        study_programs: List[StudyProgram] = self.parse_data(soup=soup)
        for study_program in study_programs:
            self.STUDY_PROGRAMS_QUEUE.put_nowait(study_program)
            if not self.STUDY_PROGRAMS_READY_EVENT.is_set():
                self.STUDY_PROGRAMS_READY_EVENT.set()
        logging.info(f"Finished processing {dataset_configuration}")
        await iceberg_client.save_data(study_programs, dataset_configuration)
        return study_programs
