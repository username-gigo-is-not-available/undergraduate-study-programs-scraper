import asyncio
import time
import logging
from concurrent.futures import Executor

from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.course_details_parser import CourseDetailsParser
from src.parsers.study_program_parser import StudyProgramParser
from src.patterns.factory import executor_factory

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    executor_type: Executor = executor_factory()
    await StudyProgramParser.process_and_save_data()
    tasks = []
    with executor_type as executor:
        tasks.append(asyncio.create_task(CurriculumParser.process_and_save_data(executor=executor)))
        tasks.append(asyncio.create_task(CourseDetailsParser.process_and_save_data(executor=executor)))
        await asyncio.gather(*tasks)
        logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
