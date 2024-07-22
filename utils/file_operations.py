import asyncio
import csv
import logging
from asyncio import Future
from concurrent.futures import Executor
from pathlib import Path

from data_models.course_details.model import CourseDetails
from data_models.curriculum.model import Curriculum
from data_models.study_program.model import StudyProgram


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


def run_save_data_to_file(data: dict[str, str | tuple], output_dir: str) -> None:
    return asyncio.run(save_data_to_file(data, output_dir))


async def save_data(executor: Executor, data: list[dict[str, str | tuple]], output_dir: Path) -> None:
    loop = asyncio.get_event_loop()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    tasks: list[Future[None]] = [loop.run_in_executor(executor, run_save_data_to_file, item, output_dir) for item in data]
    await asyncio.gather(*tasks)


async def save_data_to_file(data: dict[str, str | tuple], output_dir: str) -> None:
    file_name: str = data.get("file_name")
    rows: tuple[str | int] = data.get("rows")
    fields: tuple[str] = data.get("fields")
    logging.info(f"Saving data to file: {file_name}.csv")
    with open(f"{Path(output_dir)}/{file_name}.csv", "w", newline="", encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        writer.writerows(rows)
