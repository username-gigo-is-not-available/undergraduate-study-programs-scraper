from enum import StrEnum, auto


class CourseType(StrEnum):
    MANDATORY = auto()
    ELECTIVE = auto()

    @classmethod
    def from_bool(cls, value: bool) -> 'CourseType':
        return cls.ELECTIVE if value else cls.MANDATORY
