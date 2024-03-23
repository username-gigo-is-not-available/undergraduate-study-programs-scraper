import asyncio
import os
import time
from pathlib import Path
from typing import Any, List

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from bs4 import BeautifulSoup
import aiohttp
import logging

from constants import STUDY_PROGRAMS_2023_LI_SELECTOR
from extract.course_details import parse_course_details, parse_course_table
from extract.course_header import parse_course_tables, parse_course_header, parse_course_tables_rows
from extract.study_program import parse_study_program
from formatters import format_curriculum_data, format_course_details
from models import StudyProgram
from predicates import is_course_row, is_macedonian_study_program
from settings import STUDY_PROGRAMS_URL, ENVIRONMENT_VARIABLES, get_executor

logging.basicConfig(level=logging.INFO)


async def fetch_page(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


async def parse_study_programs() -> List[StudyProgram]:
    html = await fetch_page(STUDY_PROGRAMS_URL)
    soup = BeautifulSoup(html, 'html.parser')
    study_programs = soup.select(STUDY_PROGRAMS_2023_LI_SELECTOR)
    return [parse_study_program(study_program) for study_program in study_programs]


async def parse_curriculum_data(study_program: StudyProgram) -> pd.DataFrame:
    html = await fetch_page(study_program.url)
    soup = BeautifulSoup(html, 'html.parser')
    logging.info(f"Getting curriculum data {study_program}")
    course_tables = parse_course_tables(soup)
    course_rows = [parse_course_header(course_row) for course_row in parse_course_tables_rows(course_tables) if is_course_row(course_row)]
    return pd.DataFrame(format_curriculum_data(study_program, course_rows))


async def parse_course_data(course_urls: List[str]) -> pd.DataFrame:
    result = []
    for course_url in course_urls:
        html = await fetch_page(course_url)
        soup = BeautifulSoup(html, 'html.parser')
        course_details = parse_course_details(parse_course_table(soup))
        result.append(format_course_details(course_details))
    return pd.DataFrame(result)


def get_course_urls(df_curriculum: pd.DataFrame) -> List[str]:
    return list(df_curriculum['Course URL'].drop_duplicates())


def run_curriculum_data(study_program: StudyProgram) -> pd.DataFrame:
    return asyncio.run(parse_curriculum_data(study_program))


def run_course_data(course_urls: List[str]) -> pd.DataFrame:
    return asyncio.run(parse_course_data(course_urls))


def run_tasks_in_executor(loop: asyncio.AbstractEventLoop, executor: ThreadPoolExecutor | ProcessPoolExecutor,
                          function: callable,
                          *args: Any) -> List:
    return [loop.run_in_executor(executor, function, arg) for arg in args]


def save_data(data: dict[str, pd.DataFrame], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    for name, data_frame in data.items():
        data_frame.to_csv(Path(output_dir) / f"{name}.csv", index=False)


async def main():
    start = time.perf_counter()
    study_programs = list(filter(is_macedonian_study_program, await parse_study_programs()))
    loop = asyncio.get_event_loop()
    executor_type = get_executor()
    with executor_type as executor:
        tasks = run_tasks_in_executor(loop, executor, run_curriculum_data, *study_programs)
        df_curriculum = await asyncio.gather(*tasks)
        course_urls = get_course_urls(pd.concat(df_curriculum))
        chunk_size = len(course_urls) // len(study_programs)
        chunks = [course_urls[i:i + chunk_size] for i in range(0, len(course_urls), chunk_size)]
        tasks = run_tasks_in_executor(loop, executor, run_course_data, *chunks)
        df_course = await asyncio.gather(*tasks)

    curriculum_data = pd.concat(df_curriculum)
    course_data = pd.concat(df_course)
    result = pd.merge(curriculum_data, course_data, on=['Course Code', 'Course Name'], how='inner')
    data = {
        'curriculum': curriculum_data,
        'courses': course_data,
        'result': result
    }
    output_dir = ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '.')
    save_data(data, output_dir)
    logging.info(f"Time taken: {time.perf_counter() - start}")


if __name__ == '__main__':
    asyncio.run(main())
