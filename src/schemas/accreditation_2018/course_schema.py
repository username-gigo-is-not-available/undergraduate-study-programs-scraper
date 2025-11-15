from pyiceberg.schema import Schema
from pyiceberg.types import StringType, NestedField, IntegerType

COURSE_2018_SCHEMA = Schema(
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
        name="text",
        field_type=StringType(),
        doc="The content of the pdf file related to this course."
    )
)
