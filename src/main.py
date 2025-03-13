import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from src.config import Config
from src.parsers.course_parser import CourseParser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.study_program_parser import StudyProgramParser

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    tasks = [asyncio.create_task(StudyProgramParser.process_and_save_data())]
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser.process_and_save_data(executor=executor)))
        tasks.append(asyncio.create_task(CourseParser.process_and_save_data(executor=executor)))
        await asyncio.gather(*tasks)

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
