import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from src.configurations import ApplicationConfiguration
from src.parsers.course_parser import CourseParser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.study_program_parser import StudyProgramParser

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    tasks: list[asyncio.Task] = [asyncio.create_task(StudyProgramParser.run())]
    with ThreadPoolExecutor(max_workers=ApplicationConfiguration.MAX_WORKERS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser.run(executor=executor)))
        tasks.append(asyncio.create_task(CourseParser.run(executor=executor)))
        study_programs, curricula, courses = await asyncio.gather(*tasks)  # noqa
    logging.info(f"Study programs: {len(study_programs)}, Curricula: {len(curricula)}, Courses: {len(courses)}")
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
