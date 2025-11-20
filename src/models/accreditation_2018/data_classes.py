from dataclasses import dataclass

from src.models.enums import OfferingType


@dataclass(frozen=True)
class StudyProgram2018:
    accreditation_year: int
    name: str
    duration: int
    url: str
    title: str
    full_name: str

    def __str__(self):
        return f"{self.accreditation_year} {self.name} {self.duration}"


@dataclass(frozen=True)
class Curriculum2018:
    accreditation_year: int
    study_program_full_name: str
    course_name: str
    semester: int
    offering_type: OfferingType

    def __str__(self):
        return f"{self.accreditation_year} {self.study_program_full_name} {self.course_name}"


@dataclass(frozen=True)
class Course2018:
    accreditation_year: int
    code: str
    name: str
    url: str
    text: str

    def __str__(self):
        return f"{self.accreditation_year} {self.name}"