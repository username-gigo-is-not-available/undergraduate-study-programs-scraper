from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class OfferingType(UpperStrEnum):
    MANDATORY = auto()
    ELECTIVE = auto()

    @classmethod
    def from_parity(cls, value: int) -> 'OfferingType':
        return cls.MANDATORY if value % 2 else cls.ELECTIVE

class FileIOType(UpperStrEnum):
    S3 = auto()
    LOCAL = auto()