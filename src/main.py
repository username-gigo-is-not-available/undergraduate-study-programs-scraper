import asyncio
import logging
import ssl
import time
import src.setup
from src.initialization import initialize
from concurrent.futures import ThreadPoolExecutor
import certifi
from aiohttp import ClientSession

from src.configurations import ApplicationConfiguration, STUDY_PROGRAMS_DATASET_CONFIGURATION, COURSES_DATASET_CONFIGURATION, \
    CURRICULA_DATASET_CONFIGURATION
from src.network import HTTPClient
from src.parsers.course_parser import CourseParser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.study_program_parser import StudyProgramParser
from src.storage import IcebergClient

logging.basicConfig(level=logging.INFO, force=True)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    await initialize()
    session: ClientSession = ClientSession()
    ssl_context: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())
    http_client: HTTPClient = HTTPClient()
    iceberg_client: IcebergClient = IcebergClient()
    tasks: list[asyncio.Task] = [asyncio.create_task(StudyProgramParser().run(session=session,
                                                                              ssl_context=ssl_context,
                                                                              dataset_configuration=STUDY_PROGRAMS_DATASET_CONFIGURATION,
                                                                              http_client=http_client,
                                                                              iceberg_client=iceberg_client))]
    with ThreadPoolExecutor(max_workers=ApplicationConfiguration.NUMBER_OF_THREADS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser().run(session=session,
                                                                ssl_context=ssl_context,
                                                                executor=executor,
                                                                dataset_configuration=CURRICULA_DATASET_CONFIGURATION,
                                                                http_client=http_client,
                                                                iceberg_client=iceberg_client
                                                                )))
        tasks.append(asyncio.create_task(CourseParser().run(session=session,
                                                            ssl_context=ssl_context,
                                                            executor=executor,
                                                            dataset_configuration=COURSES_DATASET_CONFIGURATION,
                                                            http_client=http_client,
                                                            iceberg_client=iceberg_client)))
        await asyncio.gather(*tasks)
    await session.close()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
