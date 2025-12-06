from dataclasses import dataclass

from src.models.enums import OfferingType


@dataclass(frozen=True)
class StudyProgram:
    accreditation_year: int
    name: str
    duration: int
    url: str
    title: str

    def __str__(self):
        return f"{self.accreditation_year} {self.name} {self.duration}"


@dataclass(frozen=True)
class Curriculum:
    accreditation_year: int
    study_program_name: str
    study_program_duration: int
    course_name: str
    semester: int
    offering_type: OfferingType

    def __str__(self):
        return f"{self.accreditation_year} {self.study_program_name} {self.study_program_duration} {self.course_name}"


@dataclass(frozen=True)
class Course:
    accreditation_year: int
    name: str
    url: str
    code: str | None = None
    professors: str | None = None
    prerequisites: str | None = None
    text: str | None = None

    def __str__(self):
        return f"{self.accreditation_year} {self.name}"