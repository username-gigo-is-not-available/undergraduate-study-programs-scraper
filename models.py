from typing import NamedTuple

StudyProgram = NamedTuple('StudyProgram', [('name', str), ('duration', int), ('url', str)])

CourseHeader = NamedTuple('CourseHeader', [
    ('code', str),
    ('name', str),
    ('url', str),
    ('type', str),
])

CourseDetails = NamedTuple('CourseDetails', [
    ('code', str),
    ('name', str),
    ('season', str),
    ('semester', int),
    ('professors', str),
    ('prerequisite', str)
])
