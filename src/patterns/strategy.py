import asyncio
import contextlib
import csv
import logging
from concurrent.futures import Executor
from functools import partial
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, NamedTuple

import pandas as pd
from minio import S3Error

from src.config import Config
from src.patterns.common import ThreadSafetyMixin


class StorageStrategy:
    async def save_data(self, data: pd.DataFrame, output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        raise NotImplementedError


class LocalStorage(StorageStrategy):

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:
            with open(f"{Path(Config.OUTPUT_DIRECTORY_PATH / output_file_name)}", "w", newline="", encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(column_order)
                writer.writerows(data)
            return data
        except OSError as e:
            logging.error(f"Failed to save data to file {output_file_name}: {e}")


class MinioStorage(StorageStrategy):
    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        try:

            string_buffer: StringIO = StringIO()
            writer = csv.writer(string_buffer)
            writer.writerow(column_order)
            writer.writerows(data)

            bytes_buffer: BinaryIO = BytesIO()
            bytes_buffer.write(string_buffer.getvalue().encode('utf-8'))
            data_length = bytes_buffer.tell()
            bytes_buffer.seek(0)

            logging.info(f"Saving data to MinIO bucket {Config.MINIO_BUCKET_NAME} as {output_file_name}")

            Config.MINIO_CLIENT.put_object(
                bucket_name=Config.MINIO_BUCKET_NAME,
                object_name=str(output_file_name),
                data=bytes_buffer,
                length=data_length,
                content_type='text/csv'
            )
            return data
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {Config.MINIO_BUCKET_NAME}: {e}")


class ProcessingStrategy:
    @classmethod
    async def process(cls, *args, **kwargs) -> list[NamedTuple]:
        raise NotImplementedError

    @classmethod
    async def finish_processing(cls, event: asyncio.Event, done_log_msg: str) -> None:
        event.set()
        logging.info(done_log_msg)


class ProducerProcessingStrategy(ProcessingStrategy):

    @classmethod
    async def process(cls,
                      parser_function: callable,
                      output_event: asyncio.Event,
                      output_queue: asyncio.Queue,
                      producer_done_message: str
                      ) -> list[NamedTuple]:
        results: list[NamedTuple] | list[list[NamedTuple]] = await parser_function()
        for result in results:
            output_queue.put_nowait(result)

        await cls.finish_processing(output_event, producer_done_message)
        return results


class ConsumerProcessingStrategy(ProcessingStrategy, ThreadSafetyMixin):
    @classmethod
    async def wait_for_data(cls, event: asyncio.Event) -> None:
        if event:
            await event.wait()

    @classmethod
    async def is_done(cls, event: asyncio.Event, queue: asyncio.Queue) -> bool:
        return event.is_set() and queue.empty()

    @classmethod
    async def process_from_queue(cls, loop: asyncio.AbstractEventLoop,
                                 executor: Executor,
                                 parser_function: callable,
                                 input_queue: asyncio.Queue,
                                 output_queue: asyncio.Queue
                                 ) -> None:

        while not input_queue.empty():
            item: NamedTuple = input_queue.get_nowait()
            output_queue.put_nowait(
                loop.run_in_executor(executor, partial(parser_function, item=item))
            )

    @classmethod
    async def collect_from_queue(cls, flatten: bool, output_queue: asyncio.Queue) -> list[NamedTuple]:
        results: list[NamedTuple | list[NamedTuple]] = await asyncio.gather(*[output_queue.get_nowait() for _ in range(output_queue.qsize())])  # noqa
        if flatten:
            return [result for sublist in results for result in sublist]
        return results

    @classmethod
    async def process(cls,
                      parser_function: callable,
                      executor: Executor,
                      input_event: asyncio.Event,
                      input_queue: asyncio.Queue,
                      lock: asyncio.Lock,
                      output_event: asyncio.Event,
                      output_queue: asyncio.Queue,
                      consumer_done_message: str,
                      flatten: bool = False
                      ) -> list[NamedTuple]:
        loop = asyncio.get_event_loop()

        await cls.wait_for_data(input_event)

        while True:
            async with cls.async_lock(lock, executor) if lock else contextlib.nullcontext:
                if await cls.is_done(input_event, input_queue):
                    break

            await cls.process_from_queue(loop, executor, parser_function, input_queue, output_queue)

        results: list[NamedTuple] = await cls.collect_from_queue(flatten, output_queue)

        async with cls.async_lock(lock, executor) if lock else contextlib.nullcontext:
            await cls.finish_processing(output_event, consumer_done_message)

        return results
