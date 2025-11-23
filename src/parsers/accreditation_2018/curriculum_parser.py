import logging

from bs4 import Tag, BeautifulSoup

from src.models.data_classes import Curriculum, StudyProgram
from src.models.enums import OfferingType
from src.parsers.base_curriculum_parser import BaseCurriculumParser


class Curriculum2018Parser(BaseCurriculumParser):

    @property
    def accreditation_year(self) -> int:
        return 2018

    @property
    def course_section_selector(self) -> str:
        return 'div > div > div > div > div> div.view-grouping'

    @property
    def course_table_selector(self) -> str:
        return 'table'

    @property
    def course_url_selector(self) -> str:
        return 'td:nth-child(1) > a'

    @property
    def course_name_selector(self) -> str:
        return 'td:nth-child(1)'


    def parse_row(self, *args, **kwargs) -> Curriculum:

        study_program: StudyProgram = kwargs.get('study_program')
        element: Tag = kwargs.get('element')
        offering_type: OfferingType = kwargs.get('offering_type')
        semester: int = kwargs.get('semester')
        course_url: str = self.extract_url(element, self.course_url_selector)
        course_name: str = self.extract_text(element, self.course_name_selector)

        self.course_urls_queue.put_nowait(course_url)
        self.set_event(self.ready_event)

        curriculum: Curriculum = Curriculum(
            accreditation_year=self.accreditation_year,
            study_program_name=study_program.name,
            study_program_duration=study_program.duration,
            course_name=course_name,
            offering_type=offering_type,
            semester=semester
        )
        logging.info(f"Scraped curriculum {curriculum}")

        return curriculum

    def parse_data(self, *args, **kwargs) -> list[Curriculum]:

        study_program: StudyProgram = kwargs.get('study_program')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = self.get_parsed_html(page_content)

        curricula: list[Curriculum] = []
        sections: list[Tag] = soup.select(self.course_section_selector)

        for semester, section in enumerate(sections, start=1):
            tables: list[Tag] = section.select(self.course_table_selector)
            for offering_type_parity, table in enumerate(tables, start=1):
                offering_type: OfferingType = OfferingType.from_parity(offering_type_parity)
                rows: list[Tag] = self.extract_course_rows_from_section(table)
                curricula.extend(
                    self.parse_rows(
                        rows=rows,
                        offering_type=offering_type,
                        semester=semester,
                        study_program=study_program
                    )
                )

        return curricula

