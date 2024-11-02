from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.config import Config


def executor_factory() -> ThreadPoolExecutor | ProcessPoolExecutor:
    if Config.EXECUTOR_TYPE == 'THREAD':
        return ThreadPoolExecutor(max_workers=Config.MAX_WORKERS)
    elif Config.EXECUTOR_TYPE == 'PROCESS':
        return ProcessPoolExecutor(max_workers=Config.MAX_WORKERS)
    else:
        raise ValueError('Invalid executor type')
