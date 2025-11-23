import io
import io
import logging
import re

import fitz
from bs4 import Tag, BeautifulSoup

from src.models.data_classes import Course
from src.parsers.base_course_parser import BaseCourseParser
from src.parsers.base_parser import BaseParser


class Course2018Parser(BaseCourseParser):


    @property
    def accreditation_year(self) -> int:
        return 2018

    @property
    def main_selector(self) -> str:
        return '.row > section'

    @property
    def code_selector(self) -> str:
        return 'div > div > div'

    @property
    def name_selector(self) -> str:
        return 'div > ul > li'

    @property
    def arrow_selector(self) -> str:
        return 'span'


    def parse_row(self, *args, **kwargs) -> Course | None:
        url: str = kwargs.get('url')
        stream: bytes = kwargs.get('stream')
        element: Tag = kwargs.get('element')
        code: str = self.extract_text(element, self.code_selector)
        self.decompose(element, self.arrow_selector)
        if not re.search(self.code_regex(self.accreditation_year), code) and code:
            logging.info(f"Failed to process course {url}")
            return None

        document: fitz.Document | None = fitz.open(stream=io.BytesIO(stream)) if stream else None
        text: str = "".join([page.get_text() for page in document]) if document else ""

        course: Course = Course(
            accreditation_year=self.accreditation_year,
            code=code,
            name=self.extract_text(element, self.name_selector),
            url=url,
            text=text,
        )
        logging.info(f"Scraped course {course}")
        return course

    def parse_data(self, *args, **kwargs) -> Course:
        url: str = kwargs.get('url')
        stream: bytes = kwargs.get('stream', b'')
        page_content: str = kwargs.get('page_content')
        soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
        course_section: Tag = soup.select_one(self.main_selector)
        return self.parse_row(element=course_section, url=url, stream=stream)

