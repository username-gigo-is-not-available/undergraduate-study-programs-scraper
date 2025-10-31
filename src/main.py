import asyncio
import logging
import ssl
import time
from concurrent.futures import ThreadPoolExecutor

from src.configurations import STUDY_PROGRAMS, ApplicationConfiguration, CURRICULA, COURSES
from src.initialization import initialize
import certifi
from aiohttp import ClientSession

from src.network import HTTPClient
from src.parsers.accreditation_2023.course_parser import CourseParser
from src.parsers.accreditation_2023.curriculum_parser import CurriculumParser
from src.parsers.accreditation_2023.study_program_parser import StudyProgramParser
# from src.parsers.accreditation_2018.study_program_parser import StudyProgramParser
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
    semaphore: asyncio.Semaphore = asyncio.Semaphore(ApplicationConfiguration.NUMBER_OF_CONCURRENT_REQUESTS)
    tasks: list[asyncio.Task] = [asyncio.create_task(StudyProgramParser().run(session=session,
                                                                              ssl_context=ssl_context,
                                                                              iceberg_configuration=STUDY_PROGRAMS,
                                                                              http_client=http_client,
                                                                              semaphore=semaphore,
                                                                              iceberg_client=iceberg_client))]
    with ThreadPoolExecutor(max_workers=ApplicationConfiguration.NUMBER_OF_THREADS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser().run(session=session,
                                                                ssl_context=ssl_context,
                                                                executor=executor,
                                                                iceberg_configuration=CURRICULA,
                                                                http_client=http_client,
                                                                iceberg_client=iceberg_client
                                                                )))
        tasks.append(asyncio.create_task(CourseParser().run(session=session,
                                                            ssl_context=ssl_context,
                                                            executor=executor,
                                                            iceberg_configuration=COURSES,
                                                            http_client=http_client,
                                                            semaphore=semaphore,
                                                            iceberg_client=iceberg_client)))
        await asyncio.gather(*tasks)
    await session.close()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
