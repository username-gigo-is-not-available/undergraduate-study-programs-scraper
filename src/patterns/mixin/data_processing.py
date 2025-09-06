import asyncio
from typing import NamedTuple


class ProcessingMixin:

    @classmethod
    async def parse_row(cls, *args, **kwargs) -> NamedTuple:
        pass

    @classmethod
    async def parse_data(cls, *args, **kwargs) -> list[NamedTuple]:
        pass

