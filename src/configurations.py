import os
import re
from pathlib import Path

from pyiceberg.schema import Schema

from src.models.enums import FileIOType
from src.schemas.course_schema import COURSE_SCHEMA
from src.schemas.curriculum_schema import CURRICULUM_SCHEMA
from src.schemas.study_program_schema import STUDY_PROGRAM_SCHEMA
from src.setup import ENVIRONMENT_VARIABLES


class ApplicationConfiguration:
    BASE_URL: str = "https://finki.ukim.mk"
    STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"
    COURSE_CODES_REGEX: re.Pattern[str] = re.compile(r'^F23L[1-3][SW]\d{3}')

    THREADS_PER_CPU_CORE: int = 5
    NUMBER_OF_THREADS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get(
        'NUMBER_OF_THREADS') == '-1' else (
        int(ENVIRONMENT_VARIABLES.get('NUMBER_OF_THREADS')))

    REQUESTS_TIMEOUT_SECONDS: float = float(ENVIRONMENT_VARIABLES.get('REQUESTS_TIMEOUT_SECONDS'))
    REQUEST_RETRY_COUNT: int = int(ENVIRONMENT_VARIABLES.get('REQUEST_RETRY_COUNT'))
    REQUESTS_RETRY_DELAY_SECONDS: float = float(ENVIRONMENT_VARIABLES.get("REQUESTS_RETRY_DELAY_SECONDS"))


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

class DatasetConfiguration:
    def __init__(self, dataset_name: str, schema: Schema):
        self.dataset_name = dataset_name
        self.schema = schema

    def __str__(self):
        return self.dataset_name


STUDY_PROGRAMS_DATASET_CONFIGURATION = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATASET_NAME"),
    schema=STUDY_PROGRAM_SCHEMA,
)

CURRICULA_DATASET_CONFIGURATION = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("CURRICULA_DATASET_NAME"),
    schema=CURRICULUM_SCHEMA,
)

COURSES_DATASET_CONFIGURATION = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("COURSES_DATASET_NAME"),
    schema=COURSE_SCHEMA,
)
