import asyncio
from typing import NamedTuple


class ProcessingMixin:

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

    @classmethod
    def run_parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        return asyncio.run(cls.parse_data(*args, **kwargs))
