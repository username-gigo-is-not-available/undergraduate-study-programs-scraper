import logging
from typing import NamedTuple

from fastavro import validate


class SchemaValidationMixin:

    @classmethod
    async def validate(cls, records: list[NamedTuple], schema: dict) -> list[NamedTuple]:
        records_are_valid: bool = all(validate(record._asdict(), schema) for record in records)
        if records_are_valid:
            logging.info(f"Successfully validated records with schema {schema}")
            return records
        raise ValueError(f"Validation failed for schema: {schema} and records: {records[0].__class__.__name__}")
