from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

CURRICULUM_2018_SCHEMA = Schema(
NestedField(
        id=1,
        name="accreditation_year",
        field_type=IntegerType(),
        required=True,
        doc="The accreditation year this offering belongs to"
    ),
    NestedField(
        id=2,
        name="study_program_full_name",
        field_type=StringType(),
        required=True,
        doc="The name and duration of the study program."
    ),
    NestedField(
        id=3,
        name="course_name",
        field_type=StringType(),
        required=True,
        doc="The official name of the course."
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
