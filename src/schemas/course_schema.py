from pyiceberg.schema import Schema
from pyiceberg.types import StringType, NestedField, IntegerType

COURSE_SCHEMA = Schema(
NestedField(
        id=1,
        name="accreditation_year",
        field_type=IntegerType(),
        required=True,
        doc="The accreditation year this course is offered in"
    ),
    NestedField(
        id=2,
        name="code",
        field_type=StringType(),
        required=True,
        doc="The unique identifier code for the course (pattern: ^F\\d{2}L[1-3][SW]\\d{3})."
    ),
    NestedField(
        id=3,
        name="name",
        field_type=StringType(),
        required=True,
        doc="The official name of the course."
    ),
    NestedField(
        id=4,
        name="url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official course description or page."
    ),
    NestedField(
        id=5,
        name="professors",
        field_type=StringType(),
        required=False,
        doc="A comma-separated string of the main professors teaching this course, if available."
    ),
    NestedField(
        id=6,
        name="prerequisites",
        field_type=StringType(),
        required=False,
        doc="A text description of the prerequisites required to enroll in this course. Can be null if no prerequisites are listed."
    ),
    NestedField(
        id=7,
        name="text",
        field_type=StringType(),
        doc="The content of the pdf file related to this course."
    )
)
