from pyiceberg.schema import Schema
from pyiceberg.types import StringType, NestedField

COURSE_SCHEMA = Schema(
    NestedField(
        id=1,
        name="course_code",
        field_type=StringType(),
        required=True,
        doc="The unique identifier code for the course (pattern: ^F23L[1-3][SW]\\d{3})."
    ),
    NestedField(
        id=2,
        name="course_name_mk",
        field_type=StringType(),
        required=True,
        doc="The official name of the course in Macedonian."
    ),
    NestedField(
        id=3,
        name="course_name_en",
        field_type=StringType(),
        required=True,
        doc="The official name of the course in English."
    ),
    NestedField(
        id=4,
        name="course_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official course description or page."
    ),
    NestedField(
        id=5,
        name="course_professors",
        field_type=StringType(),
        required=False,
        doc="A comma-separated string of the main professors teaching this course, if available."
    ),
    NestedField(
        id=6,
        name="course_prerequisites",
        field_type=StringType(),
        required=False,
        doc="A text description of the prerequisites required to enroll in this course. Can be null if no prerequisites are listed."
    ),
    NestedField(
        id=7,
        name="course_competence",
        field_type=StringType(),
        required=True,
        doc="A text description of the learning outcomes or competencies students will gain upon completing the course."
    ),
    NestedField(
        id=8,
        name="course_content",
        field_type=StringType(),
        required=True,
        doc="A text description of the course's lectures or study plan."
    ),
)
