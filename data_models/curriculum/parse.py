from data_models.course_header.model import CourseHeader
from data_models.curriculum.model import Curriculum
from data_models.study_program.model import StudyProgram


def parse_curriculum(study_program: StudyProgram, course_header: CourseHeader) -> Curriculum:
    return Curriculum(*study_program, *course_header)


async def parse_curriculum_data(study_program: StudyProgram, course_headers: list[CourseHeader]) -> list[Curriculum]:
    return [parse_curriculum(study_program, course_header) for course_header in course_headers]
