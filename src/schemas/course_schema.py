from pyiceberg.schema import Schema
from pyiceberg.types import StringType, NestedField

COURSE_SCHEMA = Schema(
    NestedField(
        id=1,
        name="code",
        field_type=StringType(),
        required=True,
        doc="The unique identifier code for the course (pattern: ^F23L[1-3][SW]\\d{3})."
    ),
    NestedField(
        id=2,
        name="name",
        field_type=StringType(),
        required=True,
        doc="The official name of the course."
    ),
    NestedField(
        id=3,
        name="url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official course description or page."
    ),
    NestedField(
        id=4,
        name="professors",
        field_type=StringType(),
        required=False,
        doc="A comma-separated string of the main professors teaching this course, if available."
    ),
    NestedField(
        id=5,
        name="prerequisites",
        field_type=StringType(),
        required=False,
        doc="A text description of the prerequisites required to enroll in this course. Can be null if no prerequisites are listed."
    ),
    NestedField(
        id=6,
        name="competence",
        field_type=StringType(),
        required=True,
        doc="A text description of the learning outcomes or competencies students will gain upon completing the course."
    ),
    NestedField(
        id=7,
        name="content",
        field_type=StringType(),
        required=True,
        doc="A text description of the course's lectures or study plan."
    ),
)
