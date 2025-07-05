import os
import re
from pathlib import Path
from dotenv import dotenv_values

from src.models.enums import DatasetType
from src.models.named_tuples import CourseDetails, CurriculumHeader, StudyProgram

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
    MINIO_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_BUCKET_NAME')
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get('MINIO_SECURE_CONNECTION'))

    OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '..'))


class DatasetPathConfiguration:

    STUDY_PROGRAMS_OUTPUT: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'))
    CURRICULA_OUTPUT: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_DATA_OUTPUT_FILE_NAME'))
    COURSES_OUTPUT: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_DATA_OUTPUT_FILE_NAME'))


class DatasetIOConfiguration:
    def __init__(self, file_name: str | Path):
        self.file_name = file_name


class DatasetTransformationConfiguration:

    def __init__(self, columns: list[str]):
        self.columns = columns


class DatasetConfiguration:

    def __init__(self,
                 dataset: DatasetType,
                 output_io_config: DatasetIOConfiguration,
                 output_transformation_config: DatasetTransformationConfiguration,
                 ):
        self.dataset_name = dataset
        self.output_io_config = output_io_config
        self.output_transformation_config = output_transformation_config


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(DatasetType.STUDY_PROGRAMS,
                                                               DatasetIOConfiguration(DatasetPathConfiguration.STUDY_PROGRAMS_OUTPUT),
                                                               DatasetTransformationConfiguration(
                                                                   list(StudyProgram._fields))
                                                               )
DatasetConfiguration.COURSES = DatasetConfiguration(DatasetType.COURSES,
                                                               DatasetIOConfiguration(DatasetPathConfiguration.COURSES_OUTPUT),
                                                               DatasetTransformationConfiguration(
                                                                   list(CourseDetails._fields))
                                                               )
DatasetConfiguration.CURRICULA = DatasetConfiguration(DatasetType.CURRICULA,
                                                               DatasetIOConfiguration(DatasetPathConfiguration.CURRICULA_OUTPUT),
                                                               DatasetTransformationConfiguration(
                                                                   list(CurriculumHeader._fields))
                                                               )
