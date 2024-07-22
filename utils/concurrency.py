from threading import Lock

from data_models.course_details.model import CourseDetails
from data_models.course_header.model import CourseHeader
from data_models.curriculum.model import Curriculum
from data_models.study_program.model import StudyProgram
from static import MAX_WORKERS

lock: Lock = Lock()


def get_unique_course_headers(curriculums: list[Curriculum]) -> list[CourseHeader]:
    result: dict[str, CourseHeader] = {}
    for curriculum in curriculums:
        lock.acquire()
        try:
            if curriculum.course_code not in result.keys():
                result.update(
                    {
                        curriculum.course_code:
                            CourseHeader(
                                course_code=curriculum.course_code,
                                course_name_mk=curriculum.course_name_mk,
                                course_url=curriculum.course_url,
                                course_type=curriculum.course_type
                            )
                    }
                )
        finally:
            lock.release()
    return list(result.values())


def get_data_chunk_size(data: list) -> int:
    return len(data) // MAX_WORKERS


def split_data_into_chunks(data: list) -> list[list[CourseHeader | Curriculum | CourseDetails | StudyProgram]]:
    chunk_size: int = get_data_chunk_size(data)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
