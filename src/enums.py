from enum import  StrEnum


class CourseType(StrEnum):
    MANDATORY = 'Задолжителен'
    ELECTIVE = 'Изборен'

    @classmethod
    def from_bool(cls, value: bool) -> 'CourseType':
        return cls.ELECTIVE if value else cls.MANDATORY
