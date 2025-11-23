import logging

from bs4 import Tag, BeautifulSoup

from src.models.data_classes import Curriculum, StudyProgram
from src.models.enums import OfferingType
from src.parsers.base_curriculum_parser import BaseCurriculumParser


class Curriculum2023Parser(BaseCurriculumParser):

    @property
    def accreditation_year(self) -> int:
        return 2023

    @property
    def mandatory_course_table_selector(self) -> str:
        return '.col-md-6.col-sm-12'

    @property
    def elective_course_table_selector(self) -> str:
        return '.col-md-12.col-sm-12'

    @property
    def course_url_selector(self) -> str:
        return 'td:nth-child(2) > a'

    @property
    def course_name_selector(self) -> str:
        return 'td:nth-child(2)'

    @property
    def mandatory_course_semester_selector(self) -> str:
        return 'h3 > span'

    @property
    def elective_course_semester_selector(self) -> str:
        return 'td:nth-child(3)'

    def parse_row(self, *args, **kwargs) -> Curriculum:

        study_program: StudyProgram = kwargs.get('study_program')
        element: Tag = kwargs.get('element')
        offering_type: OfferingType = kwargs.get('offering_type')
        course_url: str = self.extract_url(element, self.course_url_selector)
        semester: int = kwargs.get('semester') or int(self.extract_text(element, self.elective_course_semester_selector))
        course_name: str = self.extract_text(element, self.course_name_selector)
        self.course_urls_queue.put_nowait(course_url)
        self.set_event(self.ready_event)
        curriculum: Curriculum = Curriculum(
            accreditation_year=self.accreditation_year,
            study_program_name=study_program.name,
            study_program_duration=study_program.duration,
            course_name=course_name,
            offering_type=offering_type,
            semester=semester,
        )
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    def parse_data(self, *args, **kwargs) -> list[Curriculum]:

        study_program: StudyProgram = kwargs.get('study_program')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = self.get_parsed_html(page_content)

        curricula: list[Curriculum] = []
        offering_type_selectors: dict[OfferingType, str] = {
            OfferingType.MANDATORY: self.mandatory_course_table_selector,
            OfferingType.ELECTIVE: self.elective_course_table_selector,
        }

        for offering_type, selector in offering_type_selectors.items():
            sections: list[Tag] = soup.select(selector)
            for section in sections:
                rows: list[Tag] = self.extract_course_rows_from_section(section)
                semester: str = self.extract_text(section,
                                                        self.mandatory_course_semester_selector
                                                        )
                semester: int = int(semester) if semester.isdigit() else 0
                curricula.extend(self.parse_rows(
                    rows=rows,
                    offering_type=offering_type,
                    semester=semester,
                    study_program=study_program,
                ))


        return curricula

