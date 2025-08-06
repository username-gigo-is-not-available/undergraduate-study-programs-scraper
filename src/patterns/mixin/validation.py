import logging
from pathlib import Path
from typing import NamedTuple

from fastavro import validate

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.patterns.strategy.validation import LocalStorageSchemaValidationStrategy, MinioStorageSchemaValidationStrategy


class SchemaValidationMixin:

    @classmethod
    def get_schema_validation_strategy(cls) -> type[LocalStorageSchemaValidationStrategy | MinioStorageSchemaValidationStrategy]:
        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            return LocalStorageSchemaValidationStrategy
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            return MinioStorageSchemaValidationStrategy
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    @classmethod
    async def load_schema(cls, configuration: DatasetConfiguration) -> dict:
        return await cls.get_schema_validation_strategy().load_schema(configuration.schema_configuration.file_name)

    @classmethod
    async def validate(cls, records: list[NamedTuple], schema: dict) -> list[NamedTuple]:
        records_are_valid: bool = all(validate(record._asdict(), schema) for record in records)
        if records_are_valid:
            logging.info(f"Successfully validated records with schema {schema}")
            return records
        raise ValueError(f"Validation failed for schema: {schema} and records: {records[0].__class__.__name__}")
