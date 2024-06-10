# https://finki.ukim.mk/mk/dodiplomski-studii

STUDY_PROGRAMS_2023_LI_SELECTOR = '#block-views-akreditacija-2023-block-1 > div > div > div > html > body > div > div > ul > li'
STUDY_PROGRAM_URL_SELECTOR = 'div > a[href]'
STUDY_PROGRAM_NAME_SELECTOR = 'span:nth-child(1)'
STUDY_PROGRAM_DURATION_SELECTOR = 'span:nth-child(2)'

# https://finki.ukim.mk/program/{program_name}

COURSE_TABLES_CLASS_NAME = 'table.table-striped.table.table-bordered.table-sm'
COURSE_TABLE_ROWS_SELECTOR = 'tr'
COURSE_HEADER_CODE_SELECTOR = 'td:nth-child(1)'
COURSE_HEADER_NAME_AND_URL_SELECTOR = 'td:nth-child(2) > a'
COURSE_HEADER_SUGGESTED_SEMESTER_SELECTOR = 'td:nth-child(3)'
COURSE_HEADER_ELECTIVE_GROUP_SELECTOR = 'td:nth-child(4)'

# https://finki.ukim.mk/subject/{course_code}

COURSE_DETAILS_NAME_SELECTOR = 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'
COURSE_DETAILS_CODE_SELECTOR = 'tr:nth-child(2) > td:nth-child(3) > p > span'
COURSE_DETAILS_URL_SELECTOR = 'head > link:nth-child(7)'
COURSE_DETAILS_PROFESSORS_SELECTOR = 'tr:nth-child(7) > td:nth-child(3)'
COURSE_DETAILS_PREREQUISITE_SELECTOR = 'tr:nth-child(8) > td:nth-child(3)'
COURSE_DETAILS_ACADEMIC_YEAR_SELECTOR = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(1)'
COURSE_DETAILS_SEMESTER_SEASON_SELECTOR = 'tr:nth-child(6) > td:nth-child(2) > p:nth-child(2) > span:nth-child(2)'
