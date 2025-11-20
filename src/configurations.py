import os
from dataclasses import dataclass
from pathlib import Path

from pyiceberg.schema import Schema

from src.models.enums import FileIOType
from src.parsers.base_parser import BaseParser
from src.setup import ENVIRONMENT_VARIABLES

class ApplicationConfiguration:
    THREADS_PER_CPU_CORE: int = 5
    NUMBER_OF_THREADS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get(
        'NUMBER_OF_THREADS') == '-1' else (
        int(ENVIRONMENT_VARIABLES.get('NUMBER_OF_THREADS')))
    NUMBER_OF_CONCURRENT_REQUESTS: int = int(ENVIRONMENT_VARIABLES.get('NUMBER_OF_CONCURRENT_REQUESTS'))

    REQUESTS_TIMEOUT_SECONDS: float = float(ENVIRONMENT_VARIABLES.get('REQUESTS_TIMEOUT_SECONDS'))
    REQUEST_RETRY_COUNT: int = int(ENVIRONMENT_VARIABLES.get('REQUEST_RETRY_COUNT'))
    REQUESTS_RETRY_DELAY_SECONDS: float = float(ENVIRONMENT_VARIABLES.get("REQUESTS_RETRY_DELAY_SECONDS"))
    STUDY_PROGRAMS_DATASET_NAME: str = ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATASET_NAME', "study_programs")
    CURRICULA_DATASET_NAME: str = ENVIRONMENT_VARIABLES.get('CURRICULA_DATASET_NAME', "curricula")
    COURSES_DATASET_NAME: str = ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME', "courses")


@dataclass(frozen=True)
class PipelineConfiguration:
    dataset_name: str
    accreditation_year: int
    schema: Schema
    parser: BaseParser

    @property
    def table_name(self) -> str:
        return f"{self.dataset_name}_{self.accreditation_year}"

    def __str__(self):
        return self.table_name


class StorageConfiguration:
    FILE_IO_TYPE: FileIOType = FileIOType(ENVIRONMENT_VARIABLES.get('FILE_IO_TYPE').upper())
    LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH'))
    S3_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('S3_ENDPOINT_URL')
    S3_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('S3_ACCESS_KEY')
    S3_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('S3_SECRET_KEY')
    S3_ICEBERG_LAKEHOUSE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('S3_ICEBERG_LAKEHOUSE_BUCKET_NAME')
    S3_PATH_STYLE_ACCESS: bool = ENVIRONMENT_VARIABLES.get('S3_PATH_STYLE_ACCESS')
    ICEBERG_CATALOG_NAME: str = ENVIRONMENT_VARIABLES.get("ICEBERG_CATALOG_NAME")
    ICEBERG_NAMESPACE: str = ENVIRONMENT_VARIABLES.get("ICEBERG_NAMESPACE")

