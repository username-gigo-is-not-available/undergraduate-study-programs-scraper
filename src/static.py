import os
from pathlib import Path

from dotenv import dotenv_values

ENVIRONMENT_VARIABLES: dict[str, str] = dotenv_values('../.env')

BASE_URL: str = "https://finki.ukim.mk"
STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"
THREADS_PER_CPU_CORE: int = 5
COURSE_CODE_PATTERN: str = r"F23L[1-3][SW]\d{3}"
OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH'))
MAX_WORKERS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get('MAX_WORKERS') == 'MAX_WORKERS' else (
    int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS')))
EXECUTOR_TYPE: str = ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE')
