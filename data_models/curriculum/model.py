from typing import NamedTuple

Curriculum = NamedTuple('Curriculum', [
    ('study_program_name', str),
    ('study_program_duration', int),
    ('study_program_url', str),
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_type', str),
])
