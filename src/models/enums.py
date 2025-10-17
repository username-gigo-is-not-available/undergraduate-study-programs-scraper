from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class CourseType(UpperStrEnum):
    MANDATORY = auto()
    ELECTIVE = auto()

    @classmethod
    def from_bool(cls, value: bool) -> 'CourseType':
        return cls.ELECTIVE if value else cls.MANDATORY


class CourseSemesterSeasonType(UpperStrEnum):
    WINTER = auto()
    SUMMER = auto()

    @classmethod
    def from_str(cls, value: str) -> 'CourseSemesterSeasonType':
        return cls.WINTER if value == 'Зимски' else cls.SUMMER

class FileIOType(UpperStrEnum):
    S3 = auto()
    LOCAL = auto()