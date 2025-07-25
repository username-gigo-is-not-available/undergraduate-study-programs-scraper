import asyncio
import contextlib
import threading
from asyncio import AbstractEventLoop
from concurrent.futures import Executor

from src.configurations import ApplicationConfiguration


class ThreadSafetyMixin:
    @classmethod
    @contextlib.asynccontextmanager
    async def async_lock(cls, lock: threading.Lock, executor: Executor):
        loop: AbstractEventLoop = asyncio.get_running_loop()
        acquired: bool = await loop.run_in_executor(executor, lock.acquire, ApplicationConfiguration.LOCK_TIMEOUT_SECONDS)
        if not acquired:
            raise TimeoutError(f"Could not acquire lock in {ApplicationConfiguration.LOCK_TIMEOUT_SECONDS} seconds")
        try:
            yield
        finally:
            lock.release()
