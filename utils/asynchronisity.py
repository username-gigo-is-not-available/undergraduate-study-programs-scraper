import asyncio
from asyncio import AbstractEventLoop
from concurrent.futures import Executor

from data_models.course_details.model import CourseDetails
from data_models.course_details.parse import parse_course_details_data

from data_models.course_header.model import CourseHeader
from data_models.course_header.parse import parse_course_headers_data
from data_models.curriculum.model import Curriculum
from data_models.curriculum.parse import parse_curriculum_data
from data_models.study_program.model import StudyProgram
from data_models.study_program.parse import parse_study_program_data
from settings import get_max_workers
from utils.concurrency import get_unique_course_headers
from utils.filtering import is_macedonian_study_program


def run_parse_study_program_data() -> list[StudyProgram]:
    return asyncio.run(parse_study_program_data())


def run_parse_curriculum_data(study_program: StudyProgram) -> list[Curriculum]:
    course_headers: list[CourseHeader] = asyncio.run(parse_course_headers_data(study_program))
    return asyncio.run(parse_curriculum_data(study_program, course_headers))


def run_parse_course_details_data(course_headers: list[CourseHeader]) -> list[CourseDetails]:
    return asyncio.run(parse_course_details_data(course_headers))


async def get_study_programs_data() -> list[StudyProgram]:
    return list(filter(is_macedonian_study_program, await parse_study_program_data()))


async def get_course_data(executor: Executor, loop: AbstractEventLoop, curriculum_data: list[Curriculum]) -> list[CourseDetails]:
    course_headers = get_unique_course_headers(curriculum_data)
    chunk_size = len(course_headers) // get_max_workers()
    chunks = [course_headers[i:i + chunk_size] for i in
              range(0, len(course_headers), chunk_size)]
    tasks = [loop.run_in_executor(executor, run_parse_course_details_data, course_headers_chunk) for course_headers_chunk
             in chunks]
    return [course for sublist in await asyncio.gather(*tasks) for course in sublist]


async def get_curriculum_data(executor: Executor, loop: AbstractEventLoop, study_programs_data: list[StudyProgram]) -> list[Curriculum]:
    tasks = [loop.run_in_executor(executor, run_parse_curriculum_data, study_program) for study_program in
             study_programs_data]
    return [curriculum for sublist in await asyncio.gather(*tasks) for curriculum in sublist]
