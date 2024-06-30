import ssl

import aiohttp
import certifi

from bs4 import Tag

from data_models.course_details.model import CourseDetails
from data_models.curriculum.model import Curriculum
from data_models.study_program.model import StudyProgram


async def fetch_page(url: str) -> str:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=ssl_context) as response:
            return await response.text()


def parse_object(fields: dict[str, callable], element: Tag | str) -> dict[str, str | int]:
    return {field: fields[field](element) for field in fields}


def prepare_data_for_saving(study_programs_data: list[StudyProgram], curricula_data: list[Curriculum], courses_data: list[CourseDetails]) -> \
        list[dict[str, str | tuple]]:
    return [
        {
            "file_name": "study_programs",
            "rows": study_programs_data,
            "fields": StudyProgram._fields
        },
        {
            "file_name": "curricula",
            "rows": curricula_data,
            "fields": Curriculum._fields
        },
        {
            "file_name": "courses",
            "rows": courses_data,
            "fields": CourseDetails._fields
        },
    ]
