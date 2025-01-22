import asyncio
import contextlib
import threading
from concurrent.futures import Executor

from src.config import Config


class ThreadSafetyMixin:
    @classmethod
    @contextlib.asynccontextmanager
    async def async_lock(cls, lock: threading.Lock, executor: Executor):
        loop = asyncio.get_event_loop()
        acquired: bool = await loop.run_in_executor(executor, lock.acquire, Config.LOCK_TIMEOUT_SECONDS)
        if not acquired:
            raise TimeoutError(f"Could not acquire lock in {Config.LOCK_TIMEOUT_SECONDS} seconds")
        try:
            yield
        finally:
            lock.release()
