from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

CURRICULUM_SCHEMA = Schema(
    NestedField(
        id=1,
        name="study_program_name",
        field_type=StringType(),
        required=True,
        doc="The official name of the undergraduate study program."
    ),
    NestedField(
        id=2,
        name="study_program_duration",
        field_type=IntegerType(),
        required=True,
        doc="The standard duration of the study program in academic years (range: [2, 4])"
    ),
    NestedField(
        id=3,
        name="study_program_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official study program description or page."
    ),
    NestedField(
        id=4,
        name="course_code",
        field_type=StringType(),
        required=True,
        doc="The unique identifier code for the course (pattern: ^F23L[1-3][SW]\\d{3})."
    ),
    NestedField(
        id=5,
        name="course_name_mk",
        field_type=StringType(),
        required=True,
        doc="The official name of the course in Macedonian."
    ),
    NestedField(
        id=6,
        name="course_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official course description or page."
    ),
    NestedField(
        id=7,
        name="course_semester",
        field_type=IntegerType(),
        required=True,
        doc="The semester the course is offered in (range: [1, 8], depending on the study program duration)"
    ),
    NestedField(
        id=8,
        name="course_type",
        field_type=StringType(),
        required=True,
        doc="The type of the course: MANDATORY or ELECTIVE."
    ),
)
