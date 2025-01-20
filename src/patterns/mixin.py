import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from typing import NamedTuple

from bs4 import Tag

from src.decorators import validate_course
from src.enums import ProcessingType
from src.parsers.models.field_parser import FieldParser
from src.patterns.strategy import LocalStorage, MinioStorage, ProducerProcessingStrategy, ConsumerProcessingStrategy
from src.config import Config


class FileStorageMixin:

    @classmethod
    def get_file_storage_strategy(cls):
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            if not Config.OUTPUT_DIRECTORY_PATH.exists():
                Config.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            return LocalStorage()
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            if not Config.MINIO_CLIENT.bucket_exists(Config.MINIO_BUCKET_NAME):
                Config.MINIO_CLIENT.make_bucket(Config.MINIO_BUCKET_NAME)
            return MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {Config.FILE_STORAGE_TYPE}")

    @classmethod
    async def save_data(cls, data: list[NamedTuple], output_file_name: Path, column_order: list[str]) -> list[NamedTuple]:
        return await cls.get_file_storage_strategy().save_data(data, output_file_name, column_order)


class ProcessingMixin(ABC):

    @classmethod
    def get_processing_strategy(cls, processing_strategy: ProcessingType):
        if processing_strategy == ProcessingType.PRODUCER:
            return ProducerProcessingStrategy
        elif processing_strategy == ProcessingType.CONSUMER:
            return ConsumerProcessingStrategy
        else:
            raise ValueError(f"Unsupported processing strategy: {processing_strategy}")

    @classmethod
    @abstractmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        pass

    @classmethod
    @abstractmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    @abstractmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

    @classmethod
    @validate_course
    def parse_fields(cls, element: Tag) -> dict[str, str | int | bool]:
        return {field_parser.field_name: field_parser() for field_parser in cls.get_field_parsers(element)}

    @classmethod
    def run_parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        return asyncio.run(cls.parse_data(*args, **kwargs))
