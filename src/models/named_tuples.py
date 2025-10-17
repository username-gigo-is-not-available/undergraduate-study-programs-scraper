from typing import NamedTuple

StudyProgram = NamedTuple('StudyProgram', [('study_program_name', str), ('study_program_duration', int), ('study_program_url', str)])

CourseHeader = NamedTuple('CourseHeader', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
])

Curriculum = NamedTuple('Curriculum', [
    ('study_program_name', str),
    ('study_program_duration', int),
    ('study_program_url', str),
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_semester', int),
    ('course_type', str),
])

Course = NamedTuple('Course', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_name_en', str),
    ('course_professors', str),
    ('course_prerequisites', str),
    ('course_competence', str),
    ('course_content', str)
])
