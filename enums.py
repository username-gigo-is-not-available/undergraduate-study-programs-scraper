from enum import Enum


class CourseType(Enum):
    MANDATORY = 'Задолжителен'
    ELECTIVE = 'Изборен'


class CourseSeason(Enum):
    WINTER = 'Зимски'
    SUMMER = 'Летен'
