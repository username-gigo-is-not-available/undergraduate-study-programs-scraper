from threading import Lock

from data_models.course_header.model import CourseHeader
from data_models.curriculum.model import Curriculum

lock = Lock()


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
