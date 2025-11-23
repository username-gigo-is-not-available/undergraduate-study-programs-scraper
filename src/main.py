import asyncio
import logging
import ssl
import time
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import certifi
from aiohttp import ClientSession

from src.configurations import ApplicationConfiguration, TableConfiguration
from src.initialization import create_tables
from src.network import HTTPClient, TextReadingStrategy
from src.orchestrators.course_orchestrator import CourseOrchestrator
from src.orchestrators.curriculum_orchestrator import CurriculumOrchestrator
from src.orchestrators.study_program_orchestrator import StudyProgramOrchestrator
from src.parsers.accreditation_2018.course_parser import Course2018Parser
from src.parsers.accreditation_2018.curriculum_parser import Curriculum2018Parser
from src.parsers.accreditation_2018.study_program_parser import StudyProgram2018Parser
from src.parsers.accreditation_2023.course_parser import Course2023Parser
from src.parsers.accreditation_2023.curriculum_parser import Curriculum2023Parser
from src.parsers.accreditation_2023.study_program_parser import StudyProgram2023Parser
from src.schemas.course_schema import COURSE_SCHEMA
from src.schemas.curriculum_schema import CURRICULUM_SCHEMA
from src.schemas.study_program_schema import STUDY_PROGRAM_SCHEMA
from src.storage import IcebergClient

logging.basicConfig(level=logging.INFO, force=True)

MAIN_PAGE_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()

    study_program_orchestrator: StudyProgramOrchestrator = StudyProgramOrchestrator(
        table_configuration=TableConfiguration(dataset_name=ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME,
                           schema=STUDY_PROGRAM_SCHEMA),
        parsers=[StudyProgram2018Parser(), StudyProgram2023Parser()],
    )
    curriculum_orchestrator: CurriculumOrchestrator = CurriculumOrchestrator(
        table_configuration=TableConfiguration(dataset_name=ApplicationConfiguration.CURRICULA_DATASET_NAME,
                           schema=CURRICULUM_SCHEMA),
        parsers=[Curriculum2018Parser(), Curriculum2023Parser()],

    )
    course_orchestrator: CourseOrchestrator = CourseOrchestrator(
        table_configuration=TableConfiguration(dataset_name=ApplicationConfiguration.COURSES_DATASET_NAME,
                                               schema=COURSE_SCHEMA),
        parsers=[Course2018Parser(), Course2023Parser()],
    )

    await create_tables([orchestrator.table_configuration for orchestrator in [
        study_program_orchestrator,
        curriculum_orchestrator,
        course_orchestrator
    ]])


    ssl_context: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())
    http_client: HTTPClient = HTTPClient()
    iceberg_client: IcebergClient = IcebergClient()
    semaphore: asyncio.Semaphore = asyncio.Semaphore(ApplicationConfiguration.NUMBER_OF_CONCURRENT_REQUESTS)
    async with ClientSession() as session:

            http_status, page_content, url = await http_client.fetch_page(
                strategy=TextReadingStrategy(),
                session=session,
                ssl_context=ssl_context,
                url=MAIN_PAGE_URL,
            )


            with ThreadPoolExecutor(max_workers=ApplicationConfiguration.NUMBER_OF_THREADS) as executor:
                tasks: list[Coroutine[Any, Any, None]] = [
                    study_program_orchestrator.run(
                        session=session,
                        ssl_context=ssl_context,
                        page_content=page_content,
                        semaphore=semaphore,
                        http_client=http_client,
                        iceberg_client=iceberg_client,
                    ),
                    curriculum_orchestrator.run(
                        study_program_orchestrator=study_program_orchestrator,
                        executor=executor,
                        iceberg_client=iceberg_client,
                    ),
                    course_orchestrator.run(
                        curriculum_orchestrator=curriculum_orchestrator,
                        executor=executor,
                        session=session,
                        ssl_context=ssl_context,
                        semaphore=semaphore,
                        http_client=http_client,
                        iceberg_client=iceberg_client,
                    ),
                ]

                await asyncio.gather(*tasks)
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
