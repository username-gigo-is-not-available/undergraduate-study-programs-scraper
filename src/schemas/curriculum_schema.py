from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

CURRICULUM_SCHEMA = Schema(
NestedField(
        id=1,
        name="accreditation_year",
        field_type=IntegerType(),
        required=True,
        doc="The accreditation year this offering belongs to"
    ),
    NestedField(
        id=2,
        name="study_program_name",
        field_type=StringType(),
        required=True,
        doc="The official name of the undergraduate study program."
    ),
    NestedField(
        id=3,
        name="study_program_duration",
        field_type=IntegerType(),
        required=True,
        doc="The standard duration of the study program in academic years (e.g. 2, 3, 4)"
    ),
    NestedField(
        id=4,
        name="course_name",
        field_type=StringType(),
        required=True,
        doc="The official name of the course."
    ),
    NestedField(
        id=5,
        name="semester",
        field_type=IntegerType(),
        required=True,
        doc="The semester the course is offered in (range: [1, 8], depending on the study program duration)"
    ),
    NestedField(
        id=6,
        name="offering_type",
        field_type=StringType(),
        required=True,
        doc="The type of the study program offering: MANDATORY or ELECTIVE."
    ),
)
