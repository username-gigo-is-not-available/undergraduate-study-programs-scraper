from dataclasses import dataclass

from src.models.enums import OfferingType


@dataclass(frozen=True)
class StudyProgram2023:
    accreditation_year: int
    name: str
    duration: int
    url: str
    title: str

    def __str__(self):
        return f"{self.accreditation_year} {self.name} {self.duration}"


@dataclass(frozen=True)
class Curriculum2023:
    accreditation_year: int
    study_program_name: str
    study_program_duration: int
    course_name: str
    semester: int
    offering_type: OfferingType

    def __str__(self):
        return f"{self.accreditation_year} {self.study_program_name} {self.study_program_duration} {self.course_name}"


@dataclass(frozen=True)
class Course2023:
    accreditation_year: int
    code: str
    name: str
    url: str
    professors: str
    prerequisites: str

    def __str__(self):
        return f"{self.accreditation_year} {self.name}"