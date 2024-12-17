import re

from src.config import Config


class CourseValidator:

    @staticmethod
    def validate_course(fields: dict[str, str]) -> dict[str, str | None]:
        course_name = fields.get('course_name_mk') or fields.get('course_name_en')

        if course_name and re.search(Config.COURSE_CODES_REGEX, course_name):
            course_name_key = 'course_name_mk' if 'course_name_mk' in fields else 'course_name_en'
            fields.update({
                'course_code': CourseValidator.extract_course_code(course_name),
                course_name_key: CourseValidator.extract_course_name(course_name)
            })

        return fields

    @staticmethod
    def extract_course_code(course_name: str) -> str:
        return ''.join(course_name.split(' ')[0]).upper()

    @staticmethod
    def extract_course_name(course_name: str) -> str:
        return ' '.join(course_name.split(' ')[1:])
