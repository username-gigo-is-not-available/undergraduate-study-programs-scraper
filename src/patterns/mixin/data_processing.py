import asyncio
from typing import NamedTuple

from bs4 import Tag

from src.parsers.field_parser import FieldParser
from src.patterns.decorator.validate import validate_course


class ProcessingMixin:

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
