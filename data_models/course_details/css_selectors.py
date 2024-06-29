# https://finki.ukim.mk/subject/{course_code}

COURSE_TABLES_CLASS_NAME: str = 'table.table-striped.table.table-bordered.table-sm'
COURSE_DETAILS_NAME_MK_SELECTOR: str = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
COURSE_DETAILS_NAME_EN_SELECTOR: str= 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(2) > span'
COURSE_DETAILS_CODE_SELECTOR: str = 'tr:nth-child(2) > td:nth-child(3) > p > span'
COURSE_DETAILS_URL_SELECTOR: str = 'head > link:nth-child(7)'
COURSE_DETAILS_PROFESSORS_SELECTOR: str = 'tr:nth-child(7) > td:nth-child(3)'
COURSE_DETAILS_PREREQUISITE_SELECTOR: str = 'tr:nth-child(8) > td:nth-child(3)'
COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
COURSE_DETAILS_SEMESTER_SEASON_SELECTOR: str = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
