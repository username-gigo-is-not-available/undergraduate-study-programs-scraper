from models import CourseHeader, CourseDetails, StudyProgram


def format_study_program(study_program: StudyProgram) -> dict[str, str | int]:
    return {
        'Study Program Name': study_program.name,
        'Study Program Duration': study_program.duration,
        'Study Program URL': study_program.url
    }


def format_course_header(course_header: CourseHeader) -> dict[str, str | int]:
    return {
        'Course Code': course_header.code,
        'Course Name': course_header.name,
        'Course URL': course_header.url,
        'Course Type': course_header.type,
    }


def format_course_details(course_details: CourseDetails) -> dict[str, str | int]:
    return {
        'Course Code': course_details.code,
        'Course Name': course_details.name,
        'Course Season': course_details.season,
        'Course Semester': course_details.semester,
        'Course Professors': course_details.professors,
        'Course Prerequisite': course_details.prerequisite
    }


def format_curriculum_data(study_program: StudyProgram, course_headers: list[CourseHeader]) -> list[dict[str, str]]:
    return [
        dict(**format_study_program(study_program),
             **format_course_header(course_header)
             ) for course_header in
        course_headers
    ]
