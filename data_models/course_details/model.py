from typing import NamedTuple

CourseDetails = NamedTuple('CourseDetails', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_name_en', str),
    ('course_season', str),
    ('course_academic_year', int),
    ('course_professors', str),
    ('course_prerequisite', str)
])
