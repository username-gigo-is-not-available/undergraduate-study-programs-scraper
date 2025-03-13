import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from src.config import Config
from src.models.named_tuples import CurriculumDetails
from src.parsers.course_parser import CourseParser
from src.parsers.curriculum_parser import CurriculumParser
from src.parsers.study_program_parser import StudyProgramParser
from src.patterns.mixin.file_storage import FileStorageMixin

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    tasks = [asyncio.create_task(StudyProgramParser.process_and_save_data())]
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        tasks.append(asyncio.create_task(CurriculumParser.process_and_save_data(executor=executor)))
        tasks.append(asyncio.create_task(CourseParser.process_and_save_data(executor=executor)))
        study_programs, curricula, courses = await asyncio.gather(*tasks)  # noqa

    df: pd.DataFrame = pd.DataFrame(curricula).merge(pd.DataFrame(courses), on=['course_code', 'course_name_mk', 'course_url'], how='inner')
    data: list[CurriculumDetails] = [CurriculumDetails(**row) for row in df.to_dict(orient='records')]
    await FileStorageMixin.save_data(data, Config.MERGED_DATA_OUTPUT_FILE_NAME, list(CurriculumDetails._fields))

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
