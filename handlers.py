from models import CourseHeader, CourseDetails
from settings import INVALID_COURSE_CODES


def handle_invalid_course(course: CourseHeader | CourseDetails) -> CourseHeader | CourseDetails:
    if course.code in INVALID_COURSE_CODES:
        modified_code = ''.join(course.name.split(' ')[0])
        modified_name = ' '.join(course.name.split(' ')[1:])

        common_args = {'code': modified_code, 'name': modified_name}
        other_args = {key: value for key, value in course._asdict().items() if key not in common_args}

        return type(course)(**common_args, **other_args)

    return course
