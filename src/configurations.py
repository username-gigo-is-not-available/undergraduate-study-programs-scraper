import os
import re
from pathlib import Path
from dotenv import dotenv_values

from src.models.enums import DatasetType

ENVIRONMENT_VARIABLES: dict[str, str] = {**dotenv_values('../.env'), **os.environ}


class ApplicationConfiguration:
    BASE_URL: str = "https://finki.ukim.mk"
    STUDY_PROGRAMS_URL: str = "https://finki.ukim.mk/mk/dodiplomski-studii"
    COURSE_CODES_REGEX: re.Pattern[str] = re.compile(r'^F23L[1-3][SW]\d{3}')

    THREADS_PER_CPU_CORE: int = 5
    MAX_WORKERS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get(
        'MAX_WORKERS') == 'MAX_WORKERS' else (
        int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS')))
    LOCK_TIMEOUT_SECONDS: int = int(ENVIRONMENT_VARIABLES.get('LOCK_TIMEOUT_SECONDS'))
    REQUESTS_TIMEOUT_SECONDS: int = int(ENVIRONMENT_VARIABLES.get('REQUESTS_TIMEOUT_SECONDS'))


class StorageConfiguration:
    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get('FILE_STORAGE_TYPE')

    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('MINIO_ENDPOINT_URL')
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_SECRET_KEY')
    MINIO_OUTPUT_DATA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_OUTPUT_DATA_BUCKET_NAME')
    MINIO_SCHEMA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_SCHEMA_BUCKET_NAME')
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get('MINIO_SECURE_CONNECTION'))

    OUTPUT_DATA_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DATA_DIRECTORY_PATH', '..'))
    SCHEMA_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('SCHEMA_DIRECTORY_PATH', '..'))


class DatasetPathConfiguration:
    STUDY_PROGRAMS_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'))
    CURRICULA_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_DATA_OUTPUT_FILE_NAME'))
    COURSES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_DATA_OUTPUT_FILE_NAME'))

    STUDY_PROGRAMS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_SCHEMA_FILE_NAME'))
    CURRICULA_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_SCHEMA_FILE_NAME'))
    COURSES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_SCHEMA_FILE_NAME'))


class DatasetIOConfiguration:
    def __init__(self, file_name: str | Path):
        self.file_name = file_name

class DatasetConfiguration:
    STUDY_PROGRAMS: "DatasetConfiguration"
    CURRICULA: "DatasetConfiguration"
    COURSES: "DatasetConfiguration"

    def __init__(self,
                 dataset: DatasetType,
                 output_io_config: DatasetIOConfiguration,
                 output_schema_config: DatasetIOConfiguration,
                 ):
        self.dataset_name = dataset
        self.output_io_config = output_io_config
        self.output_schema_config = output_schema_config


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(DatasetType.STUDY_PROGRAMS,
                                                           DatasetIOConfiguration(
                                                               DatasetPathConfiguration.STUDY_PROGRAMS_OUTPUT_DATA),
                                                           DatasetIOConfiguration(
                                                               DatasetPathConfiguration.STUDY_PROGRAMS_SCHEMA),
                                                           )
DatasetConfiguration.COURSES = DatasetConfiguration(DatasetType.COURSES,
                                                    DatasetIOConfiguration(
                                                        DatasetPathConfiguration.COURSES_OUTPUT_DATA),
                                                    DatasetIOConfiguration(
                                                        DatasetPathConfiguration.COURSES_SCHEMA),

                                                    )
DatasetConfiguration.CURRICULA = DatasetConfiguration(DatasetType.CURRICULA,
                                                      DatasetIOConfiguration(
                                                          DatasetPathConfiguration.CURRICULA_OUTPUT_DATA),
                                                      DatasetIOConfiguration(
                                                          DatasetPathConfiguration.CURRICULA_SCHEMA),
                                                      )
