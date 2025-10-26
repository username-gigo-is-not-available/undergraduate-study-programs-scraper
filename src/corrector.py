import re

from src.configurations import ApplicationConfiguration


class CourseCorrector:

    @staticmethod
    def correct(fields: dict[str, str]) -> dict[str, str | None]:
        course_name = fields.get('name')

        if course_name and re.search(ApplicationConfiguration.COURSE_CODES_REGEX, course_name):
            fields.update({
                'code': CourseCorrector.extract_course_code(course_name),
                'name': CourseCorrector.extract_course_name(course_name)
            })

        return fields

    @staticmethod
    def extract_course_code(course_name: str) -> str:
        return ''.join(course_name.split(' ')[0]).upper()

    @staticmethod
    def extract_course_name(course_name: str) -> str:
        return ' '.join(course_name.split(' ')[1:])
