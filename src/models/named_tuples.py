from typing import NamedTuple

StudyProgram = NamedTuple('StudyProgram', [('study_program_name', str), ('study_program_duration', int), ('study_program_url', str)])

CourseHeader = NamedTuple('CourseHeader', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
])

PartialCurriculum = NamedTuple('PartialCurriculum', [
    ('study_program_name', str),
    ('study_program_duration', int),
    ('study_program_url', str),
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_type', str),
])

Course = NamedTuple('CourseDetails', [
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_name_en', str),
    ('course_semester_season', str),
    ('course_academic_year', int),
    ('course_professors', str),
    ('course_prerequisites', str)
])

Curriculum = NamedTuple('Curriculum', [
    ('study_program_name', str),
    ('study_program_duration', int),
    ('study_program_url', str),
    ('course_code', str),
    ('course_name_mk', str),
    ('course_url', str),
    ('course_type', str),
    ('course_name_en', str),
    ('course_semester_season', str),
    ('course_academic_year', int),
    ('course_professors', str),
    ('course_prerequisites', str)
])