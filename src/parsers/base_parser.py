import asyncio
import contextlib
import logging
import threading
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from typing import NamedTuple
from bs4 import Tag
from src.parsers.field_parser import FieldParser
from src.mixins import ThreadSafetyMixin, StorageMixin, HTTPClientMixin


class Parser(ABC, ThreadSafetyMixin, StorageMixin, HTTPClientMixin):

    @classmethod
    @abstractmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        pass

    @classmethod
    def parse_fields(cls, element: Tag) -> dict[str, str | int | bool]:
        return {field_parser.field_name: field_parser() for field_parser in cls.get_field_parsers(element)}

    @classmethod
    @abstractmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    @abstractmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

    @classmethod
    def run_parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        return asyncio.run(cls.parse_data(*args, **kwargs))

    @classmethod
    async def scrape_data(
            cls,
            output_event: asyncio.Event,
            output_queue: asyncio.Queue,
            parse_func: callable,
            done_log_msg: str,
            executor: Executor = None,
            lock: threading.Lock = None,
            input_event: asyncio.Event = None,
            input_queue: asyncio.Queue = None,
            flatten: bool = False,
            *args,
            **kwargs
    ) -> list[NamedTuple]:

        loop: AbstractEventLoop = asyncio.get_event_loop()

        if input_event:
            await input_event.wait()

        if input_queue:
            while True:
                async with cls.async_lock(lock, executor):
                    if input_event.is_set() and input_queue.empty():
                        break

                for _ in range(input_queue.qsize()):
                    item: NamedTuple = input_queue.get_nowait()
                    output_queue.put_nowait(
                        loop.run_in_executor(executor, partial(parse_func, item=item))
                    )

            results: list[NamedTuple] | list[list[NamedTuple]] = [
                result for result in await asyncio.gather(
                    *[output_queue.get_nowait() for _ in range(output_queue.qsize())]
                )
            ]

        else:
            results: list[NamedTuple] | list[list[NamedTuple]] = await parse_func()
            for result in results:
                output_queue.put_nowait(result)

        if flatten:
            results: list[NamedTuple] = [item for sublist in results for item in sublist]

        if lock:
            async with cls.async_lock(lock, executor):
                output_event.set()
                logging.info(done_log_msg)

        else:
            output_event.set()
            logging.info(done_log_msg)

        return results

    @classmethod
    async def scrape_and_save_data(cls,
                                   file_name: Path,
                                   column_order: list[str],
                                   output_event: asyncio.Event,
                                   output_queue: asyncio.Queue,
                                   parse_func: callable,
                                   done_log_msg: str,
                                   executor: Executor = None,
                                   lock: threading.Lock = None,
                                   input_event: asyncio.Event = None,
                                   input_queue: asyncio.Queue = None,
                                   flatten: bool = False,
                                   *args,
                                   **kwargs
                                   ) -> list[NamedTuple]:

        data: list[NamedTuple] = await cls.scrape_data(
            output_event=output_event,
            output_queue=output_queue,
            executor=executor,
            lock=lock,
            parse_func=parse_func,
            done_log_msg=done_log_msg,
            input_event=input_event,
            input_queue=input_queue,
            flatten=flatten,
            *args,
            **kwargs)
        await cls.save_data(data=data, output_file_name=file_name, column_order=column_order)
        return data
