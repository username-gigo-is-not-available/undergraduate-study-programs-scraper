from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from dotenv import dotenv_values


BASE_URL: str = "https://finki.ukim.mk"
STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"

INVALID_COURSE_CODES: list[str] = ['F23L1S026', 'F23L2S066']

ENVIRONMENT_VARIABLES = dotenv_values(".env")

for variable_name, variable_value in ENVIRONMENT_VARIABLES.items():
    if not variable_value:
        raise RuntimeError(f"{variable_name} is not set!")


def get_max_workers():
    return None if ENVIRONMENT_VARIABLES['MAX_WORKERS'] == 'MAX_WORKERS' else int(ENVIRONMENT_VARIABLES['MAX_WORKERS'])


def get_executor():
    max_workers = get_max_workers()
    if ENVIRONMENT_VARIABLES['EXECUTOR_TYPE'] == 'THREAD':
        return ThreadPoolExecutor(max_workers=max_workers)
    elif ENVIRONMENT_VARIABLES['EXECUTOR_TYPE'] == 'PROCESS':
        return ProcessPoolExecutor(max_workers=max_workers)
    else:
        raise ValueError("Invalid executor type")
