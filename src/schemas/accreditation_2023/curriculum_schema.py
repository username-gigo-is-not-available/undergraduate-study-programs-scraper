from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

CURRICULUM_2023_SCHEMA = Schema(
NestedField(
        id=1,
        name="accreditation_year",
        field_type=IntegerType(),
        required=True,
        doc="The accreditation year this offering belongs to"
    ),
    NestedField(
        id=2,
        name="study_program_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official study program description or page."
    ),
    NestedField(
        id=3,
        name="course_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official course description or page."
    ),
    NestedField(
        id=4,
        name="semester",
        field_type=IntegerType(),
        required=True,
        doc="The semester the course is offered in (range: [1, 8], depending on the study program duration)"
    ),
    NestedField(
        id=5,
        name="offering_type",
        field_type=StringType(),
        required=True,
        doc="The type of the study program offering: MANDATORY or ELECTIVE."
    ),
)
