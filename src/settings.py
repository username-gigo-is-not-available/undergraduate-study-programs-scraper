from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from static import MAX_WORKERS, ENVIRONMENT_VARIABLES


def get_executor() -> ThreadPoolExecutor | ProcessPoolExecutor:
    if ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'THREAD':
        return ThreadPoolExecutor(max_workers=MAX_WORKERS)
    elif ENVIRONMENT_VARIABLES.get('EXECUTOR_TYPE') == 'PROCESS':
        return ProcessPoolExecutor(max_workers=MAX_WORKERS)
    else:
        raise ValueError('Invalid executor type')
