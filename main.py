import asyncio
import time
import logging

from utils.asynchronisity import get_study_programs_data, get_course_data, \
    get_curriculum_data, save_data
from settings import ENVIRONMENT_VARIABLES, get_executor
from utils.data_and_parsing import prepare_data_for_saving

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    executor_type = get_executor()
    output_directory = ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '.')
    study_programs_data = await get_study_programs_data()
    with executor_type as executor:
        curricula_data = await get_curriculum_data(executor, loop, study_programs_data)
        courses_data = await get_course_data(executor, loop, curricula_data)
        data = prepare_data_for_saving(study_programs_data, curricula_data, courses_data)
        await save_data(executor, loop, data, output_directory)

    logging.info(f"Time taken: {time.perf_counter() - start}")


if __name__ == '__main__':
    asyncio.run(main())
