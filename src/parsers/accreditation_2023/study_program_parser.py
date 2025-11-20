import logging

from bs4 import Tag, BeautifulSoup

from src.models.accreditation_2023.data_classes import StudyProgram2023
from src.parsers.base_parser import BaseParser
from src.parsers.base_study_program_parser import BaseStudyProgramParser


class StudyProgram2023Parser(BaseStudyProgramParser):

    @property
    def accreditation_year(self) -> int:
        return 2023

    @property
    def li_selector(self) -> str:
        return '#block-views-akreditacija-2023-block-1 > div > div > div > div > div > ul > li > div'

    @property
    def main_selector(self) -> str:
        return '#block-system-main > div > div > div > div:nth-child(7)'

    @property
    def name_selector(self) -> str:
        return 'h2 > span:nth-child(1)'

    @property
    def duration_selector(self):
        return 'h2 > span:nth-child(2)'

    @property
    def title_selector(self) -> str:
        return 'div:nth-child(3) > div:nth-child(2) > h5'

    def parse_row(self, *args, **kwargs) -> StudyProgram2023:
        element: Tag = kwargs.get('element')
        url: str = kwargs.get('url')

        study_program: StudyProgram2023 = StudyProgram2023(
            accreditation_year=self.accreditation_year,
            name=self.extract_text(element, self.name_selector),
            duration=int(self.extract_text(element, self.duration_selector)),
            url=url,
            title=self.extract_text(element, self.title_selector),
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    def parse_data(self, *args, **kwargs) -> list[StudyProgram2023]:
        page_content: str = kwargs.get('page_content')
        url: str = kwargs.get('url')
        soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.main_selector)
        return [self.parse_row(element=element, url=url)]

