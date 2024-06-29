import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from dotenv import dotenv_values

from static import THREADS_PER_CPU_CORE

ENVIRONMENT_VARIABLES: dict[str, str | None] = dotenv_values(".env")

for variable_name, variable_value in ENVIRONMENT_VARIABLES.items():
    if not variable_value:
        raise RuntimeError(f"{variable_name} is not set!")


def get_max_workers() -> int:
    return THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get('MAX_WORKERS') == 'MAX_WORKERS' else int(ENVIRONMENT_VARIABLES['MAX_WORKERS'])


def get_executor() -> ThreadPoolExecutor | ProcessPoolExecutor:
    max_workers = get_max_workers()
    if ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'THREAD':
        return ThreadPoolExecutor(max_workers=max_workers)
    elif ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'PROCESS':
        return ProcessPoolExecutor(max_workers=max_workers)
    else:
        raise ValueError('Invalid executor type')
