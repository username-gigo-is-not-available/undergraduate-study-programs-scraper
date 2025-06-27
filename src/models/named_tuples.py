from typing import NamedTuple

StudyProgram = NamedTuple('StudyProgram', [('study_program_name', str), ('study_program_duration', int), ('study_program_url', str)])

CourseHeader = NamedTuple('CourseHeader', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
])

CurriculumHeader = NamedTuple('CurriculumHeader', [
    ('study_program_name', str),
    ('study_program_duration', int),
    ('study_program_url', str),
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_semester', int),
    ('course_type', str),
])

CourseDetails = NamedTuple('CourseDetails', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_name_en', str),
    ('course_professors', str),
    ('course_prerequisites', str)
])
