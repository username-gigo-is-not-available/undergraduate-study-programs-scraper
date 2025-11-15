import os
import re
from dataclasses import dataclass
from pathlib import Path

from pyiceberg.schema import Schema

from src.models.enums import FileIOType
from src.schemas.accreditation_2018.course_schema import COURSE_2018_SCHEMA
from src.schemas.accreditation_2018.curriculum_schema import CURRICULUM_2018_SCHEMA
from src.schemas.accreditation_2018.study_program_schema import STUDY_PROGRAM_2018_SCHEMA
from src.schemas.accreditation_2023.course_schema import COURSE_2023_SCHEMA
from src.schemas.accreditation_2023.curriculum_schema import CURRICULUM_2023_SCHEMA
from src.schemas.accreditation_2023.study_program_schema import STUDY_PROGRAM_2023_SCHEMA
from src.setup import ENVIRONMENT_VARIABLES

class ApplicationConfiguration:
    BASE_URL: str = "https://finki.ukim.mk"
    STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"
    COURSE_2023_CODE_REGEX: re.Pattern[str] = re.compile(r'^F23L[1-3][SW]\d{3}')
    COURSE_2018_CODE_REGEX: re.Pattern[str] = re.compile(r'^F18L[1-3][SW]\d{2,3}')

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

class TableConfiguration:
    SCHEMA_MAP: dict[str, dict[int, Schema]] = {
        ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME: {
            2023: STUDY_PROGRAM_2023_SCHEMA,
            2018: STUDY_PROGRAM_2018_SCHEMA,
        },
        ApplicationConfiguration.CURRICULA_DATASET_NAME: {
            2023: CURRICULUM_2023_SCHEMA,
            2018: CURRICULUM_2018_SCHEMA,
        },
        ApplicationConfiguration.COURSES_DATASET_NAME: {
            2023: COURSE_2023_SCHEMA,
            2018: COURSE_2018_SCHEMA,
        }
    }

    def __init__(self, table_name: str, schema: Schema):
        self.table_name = table_name
        self.schema = schema

    @classmethod
    def from_dataset_name_and_accreditation_year(cls, dataset_name: str, accreditation_year: int) -> 'TableConfiguration':
        table_name: str = f"{dataset_name}_{accreditation_year}"
        schema: Schema = cls.SCHEMA_MAP[dataset_name][accreditation_year]
        return cls(table_name, schema)

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


STUDY_PROGRAMS_2023: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME,
    accreditation_year=2023
)

CURRICULA_2023: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.CURRICULA_DATASET_NAME,
    accreditation_year=2023
)

COURSES_2023: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.COURSES_DATASET_NAME,
    accreditation_year=2023
)

STUDY_PROGRAMS_2018: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.STUDY_PROGRAMS_DATASET_NAME,
    accreditation_year=2018
)

CURRICULA_2018: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.CURRICULA_DATASET_NAME,
    accreditation_year=2018
)

COURSES_2018: TableConfiguration = TableConfiguration.from_dataset_name_and_accreditation_year(
    dataset_name=ApplicationConfiguration.COURSES_DATASET_NAME,
    accreditation_year=2018
)