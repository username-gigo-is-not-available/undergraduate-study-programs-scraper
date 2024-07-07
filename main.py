import asyncio
import time
import logging
from asyncio import AbstractEventLoop
from concurrent.futures import Executor

from data_models.course_details.model import CourseDetails
from data_models.curriculum.model import Curriculum
from data_models.study_program.model import StudyProgram
from utils.data_processing import get_study_programs_data, get_curriculum_data, get_course_data
from settings import ENVIRONMENT_VARIABLES, get_executor
from utils.files import prepare_data_for_saving, save_data

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start: float = time.perf_counter()
    loop: AbstractEventLoop = asyncio.get_event_loop()
    executor_type: Executor = get_executor()
    output_directory: str = ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '.')
    study_programs_data: list[StudyProgram] = await get_study_programs_data()
    with executor_type as executor:
        curricula_data: list[Curriculum] = await get_curriculum_data(executor, study_programs_data)
        courses_data: list[CourseDetails] = await get_course_data(executor, curricula_data)
        data: list[dict[str, str | tuple]] = prepare_data_for_saving(study_programs_data, curricula_data, courses_data)
        await save_data(executor, data, output_directory)

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
