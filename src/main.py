import asyncio
import logging
import ssl
import time
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import certifi
from aiohttp import ClientSession

from src.configurations import ApplicationConfiguration
from src.parsers.course_parser import CourseParser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.study_program_parser import StudyProgramParser

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    session: ClientSession = aiohttp.ClientSession()
    ssl_context: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())
    tasks: list[asyncio.Task] = [asyncio.create_task(StudyProgramParser().run(session=session, ssl_context=ssl_context))]
    with ThreadPoolExecutor(max_workers=ApplicationConfiguration.NUMBER_OF_THREADS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser().run(session=session, ssl_context=ssl_context, executor=executor)))
        tasks.append(asyncio.create_task(CourseParser().run(session=session, ssl_context=ssl_context, executor=executor)))
        await asyncio.gather(*tasks)
    await session.close()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
