import asyncio
import logging
import ssl
import time
from concurrent.futures import ThreadPoolExecutor

from pyiceberg.schema import Schema

from src.configurations import ApplicationConfiguration, PipelineConfiguration
from src.initialization import initialize
import certifi
from aiohttp import ClientSession

from src.network import HTTPClient, TextReadingStrategy
from src.parsers.accreditation_2018.course_parser import Course2018Parser
from src.parsers.accreditation_2018.curriculum_parser import Curriculum2018Parser
from src.parsers.accreditation_2018.study_program_parser import StudyProgram2018Parser
from src.parsers.accreditation_2023.course_parser import Course2023Parser
from src.parsers.accreditation_2023.curriculum_parser import Curriculum2023Parser
from src.parsers.accreditation_2023.study_program_parser import StudyProgram2023Parser
from src.parsers.base_parser import BaseParser

from src.schemas.accreditation_2018.course_schema import COURSE_2018_SCHEMA
from src.schemas.accreditation_2018.curriculum_schema import CURRICULUM_2018_SCHEMA
from src.schemas.accreditation_2018.study_program_schema import STUDY_PROGRAM_2018_SCHEMA
from src.schemas.accreditation_2023.course_schema import COURSE_2023_SCHEMA
from src.schemas.accreditation_2023.curriculum_schema import CURRICULUM_2023_SCHEMA
from src.schemas.accreditation_2023.study_program_schema import STUDY_PROGRAM_2023_SCHEMA
from src.storage import IcebergClient

logging.basicConfig(level=logging.INFO, force=True)

MAIN_PAGE_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"

dataset_parsers: dict[str, dict[int, tuple[BaseParser, Schema]]] = {
    ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME: {
        2018: (StudyProgram2018Parser(), STUDY_PROGRAM_2018_SCHEMA),
        2023: (StudyProgram2023Parser(), STUDY_PROGRAM_2023_SCHEMA),
    },
    ApplicationConfiguration.CURRICULA_DATASET_NAME: {
        2018: (Curriculum2018Parser(), CURRICULUM_2018_SCHEMA),
        2023: (Curriculum2023Parser(), CURRICULUM_2023_SCHEMA),
    },
    ApplicationConfiguration.COURSES_DATASET_NAME: {
        2018: (Course2018Parser(), COURSE_2018_SCHEMA),
        2023: (Course2023Parser(), COURSE_2023_SCHEMA),
    },
}

execution_configurations: dict[str, dict[int, PipelineConfiguration]]= {
    dataset: {
        year: PipelineConfiguration(
            dataset_name=dataset,
            accreditation_year=year,
            schema=schema,
            parser=parser
        )
        for year, (parser, schema) in years.items()
    }
    for dataset, years in dataset_parsers.items()
}


pipeline_configurations: list[PipelineConfiguration] = [
                                      configuration
                                      for year_configuration in execution_configurations.values()
                                      for configuration in year_configuration.values()
            ]

accreditation_years: set[int] = {
    year
    for configuration in execution_configurations.values()
    for year in configuration.keys()
}

async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    await initialize(pipeline_configurations)
    session: ClientSession = ClientSession()
    ssl_context: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())
    http_client: HTTPClient = HTTPClient()
    iceberg_client: IcebergClient = IcebergClient()
    semaphore: asyncio.Semaphore = asyncio.Semaphore(ApplicationConfiguration.NUMBER_OF_CONCURRENT_REQUESTS)
    executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=ApplicationConfiguration.NUMBER_OF_CONCURRENT_REQUESTS)
    tasks: list[asyncio.Task] = []
    http_status, page_content, url = await http_client.fetch_page(
        strategy=TextReadingStrategy(),
        session=session,
        ssl_context=ssl_context,
        url=MAIN_PAGE_URL,
    )
    for accreditation_year in accreditation_years:
        study_program_configuration: PipelineConfiguration = execution_configurations[ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME][accreditation_year]
        curriculum_configuration: PipelineConfiguration = execution_configurations[ApplicationConfiguration.CURRICULA_DATASET_NAME][accreditation_year]
        course_configuration: PipelineConfiguration = execution_configurations[ApplicationConfiguration.COURSES_DATASET_NAME][accreditation_year]
        tasks.append(
            asyncio.create_task(study_program_configuration.parser.run(
                                                         session=session,
                                                         ssl_context=ssl_context,
                                                         page_content=page_content,
                                                         table_name=study_program_configuration.table_name,
                                                         schema=study_program_configuration.schema,
                                                         iceberg_client=iceberg_client,
                                                         http_client=http_client,
                                                         semaphore=semaphore
                                                         )

            )
        )

        tasks.append(asyncio.create_task(curriculum_configuration.parser.run(
                                                                    table_name=curriculum_configuration.table_name,
                                                                    schema=curriculum_configuration.schema,
                                                                    iceberg_client=iceberg_client,
                                                                    study_program_parser=study_program_configuration.parser,
                                                                    executor=executor
                                                                    )
            )
        )

        tasks.append(asyncio.create_task(course_configuration.parser.run(
                                                            session=session,
                                                            ssl_context=ssl_context,
                                                            table_name=course_configuration.table_name,
                                                            schema=course_configuration.schema,
                                                            executor=executor,
                                                            iceberg_client=iceberg_client,
                                                            http_client=http_client,
                                                            curriculum_parser=curriculum_configuration.parser,
                                                            semaphore=semaphore,
                                                            )
            )
        )

    await asyncio.gather(*tasks)
    await session.close()
    executor.shutdown(wait=True)
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
