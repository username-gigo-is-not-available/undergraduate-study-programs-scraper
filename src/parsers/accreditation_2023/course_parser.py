import logging

from bs4 import Tag, BeautifulSoup

from src.models.accreditation_2023.data_classes import Course2023
from src.parsers.base_course_parser import BaseCourseParser
from src.parsers.base_parser import BaseParser


class Course2023Parser(BaseCourseParser):


    @property
    def accreditation_year(self) -> int:
        return 2023

    @property
    def main_selector(self) -> str:
        return 'table.table-striped.table.table-bordered.table-sm'

    @property
    def code_selector(self) -> str:
        return 'tr:nth-child(2) > td:nth-child(3) > p > span'

    @property
    def name_selector(self) -> str:
        return 'tr:nth-child(1) > td:nth-child(3) > p:nth-child(1) > b'

    @property
    def professors_selector(self) -> str:
        return 'tr:nth-child(7) > td:nth-child(3) > p > span:nth-child(even)'

    @property
    def prerequisites_selector(self) -> str:
        return 'tr:nth-child(8) > td:nth-child(3) > p > span'

    def parse_row(self, *args, **kwargs) -> Course2023:
        url: str = kwargs.get('url')
        element: Tag = kwargs.get('element')

        course: Course2023 = Course2023(
            accreditation_year=self.accreditation_year,
            code=self.extract_text(element, self.code_selector),
            name=self.extract_text(element, self.name_selector),
            url=url,
            professors=", ".join(self.extract_multiple_texts(element, self.professors_selector)),
            prerequisites=self.extract_text(element, self.prerequisites_selector),
        )
        logging.info(f"Scraped course {course}")
        return course

    def parse_data(self, *args, **kwargs) -> Course2023:
        url = kwargs.get('url')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.main_selector)
        return self.parse_row(url=url, element=element)
