from dataclasses import dataclass

from src.models.enums import OfferingType


@dataclass(frozen=True)
class StudyProgram:
    name: str
    duration: int
    url: str
    title: str

    def __str__(self):
        return f"{self.name} {self.duration}"

@dataclass(frozen=True)
class CourseHeader:
    code: str
    name: str
    url: str

@dataclass(frozen=True)
class Curriculum:
    study_program_url: str
    course_url: str
    semester: int
    offering_type: OfferingType

    def __str__(self):
        return f"{self.study_program_url} {self.course_url} {self.semester} {self.offering_type}"

@dataclass(frozen=True)
class Course:
    code: str
    name: str
    url: str
    professors: str
    prerequisites: str
    competence: str
    content: str

    def __str__(self):
        return f"{self.name}"