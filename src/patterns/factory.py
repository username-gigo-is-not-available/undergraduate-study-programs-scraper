from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.static import ENVIRONMENT_VARIABLES, MAX_WORKERS


def executor_factory() -> ThreadPoolExecutor | ProcessPoolExecutor:
    if ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'THREAD':
        return ThreadPoolExecutor(max_workers=MAX_WORKERS)
    elif ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'PROCESS':
        return ProcessPoolExecutor(max_workers=MAX_WORKERS)
    else:
        raise ValueError('Invalid executor type')
