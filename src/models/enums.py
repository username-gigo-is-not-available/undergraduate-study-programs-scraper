from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class OfferingType(UpperStrEnum):
    MANDATORY = auto()
    ELECTIVE = auto()

    @classmethod
    def from_bool(cls, value: bool) -> 'OfferingType':
        return cls.ELECTIVE if value else cls.MANDATORY


class SemesterSeasonType(UpperStrEnum):
    WINTER = auto()
    SUMMER = auto()

    @classmethod
    def from_str(cls, value: str) -> 'SemesterSeasonType':
        return cls.WINTER if value == 'Зимски' else cls.SUMMER

class FileIOType(UpperStrEnum):
    S3 = auto()
    LOCAL = auto()