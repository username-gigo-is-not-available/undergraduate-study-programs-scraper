from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

STUDY_PROGRAM_SCHEMA: Schema = Schema(
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
        doc="The standard duration of the study program in academic years (e.g. 2, 3, 4)"
    ),
    NestedField(
        id=3,
        name="study_program_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official study program description or page."
    ),
)
