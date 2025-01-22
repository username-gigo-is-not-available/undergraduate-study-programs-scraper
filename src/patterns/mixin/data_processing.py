import asyncio
from typing import NamedTuple

from bs4 import Tag

from src.decorators import validate_course
from src.enums import ProcessingType
from src.parsers.models.field_parser import FieldParser
from src.patterns.strategy.data_processing import ProducerProcessingStrategy, ConsumerProcessingStrategy


class ProcessingMixin:

    @classmethod
    def get_processing_strategy(cls, processing_strategy: ProcessingType):
        if processing_strategy == ProcessingType.PRODUCER:
            return ProducerProcessingStrategy
        elif processing_strategy == ProcessingType.CONSUMER:
            return ConsumerProcessingStrategy
        else:
            raise ValueError(f"Unsupported processing strategy: {processing_strategy}")

    @classmethod
    def get_field_parsers(cls, element: Tag) -> list[FieldParser]:
        pass

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

    @classmethod
    @validate_course
    def parse_fields(cls, element: Tag) -> dict[str, str | int | bool]:
        return {field_parser.field_name: field_parser() for field_parser in cls.get_field_parsers(element)}

    @classmethod
    def run_parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        return asyncio.run(cls.parse_data(*args, **kwargs))
