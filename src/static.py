import os

from dotenv import dotenv_values

ENVIRONMENT_VARIABLES: dict[str, str] = {**dotenv_values('../.env'), **os.environ}

BASE_URL: str = "https://finki.ukim.mk"
STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"
THREADS_PER_CPU_CORE: int = 5
MAX_WORKERS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get('MAX_WORKERS') == 'MAX_WORKERS' else (
    int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS')))
EXECUTOR_TYPE: str = ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE')
