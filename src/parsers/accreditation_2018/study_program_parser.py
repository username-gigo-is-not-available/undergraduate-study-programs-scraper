import logging

from bs4 import Tag, BeautifulSoup

from src.configurations import ApplicationConfiguration
from src.models.accreditation_2018.data_classes import StudyProgram2018
from src.parsers.base_parser import BaseParser
from src.parsers.base_study_program_parser import BaseStudyProgramParser


class StudyProgram2018Parser(BaseStudyProgramParser):

    @property
    def accreditation_year(self) -> int:
        return 2018

    @property
    def li_selector(self) -> str:
        return '#block-system-main > div > div > div > ul > li > div'

    @property
    def main_selector(self) -> str:
        return 'body > div> div > div > section'

    @property
    def name_selector(self) -> str:
        return 'div > ul > li'

    @property
    def arrow_selector(self) -> str:
        return 'span'

    @property
    def duration_selector(self) -> str:
        return 'div > div > div > p:nth-child(5) > strong'

    @property
    def title_selector(self) -> str:
        return 'div > div > div > p:nth-child(5)'


    def parse_row(self, *args, **kwargs) -> StudyProgram2018:
        element: Tag = kwargs.get('element')
        url: str = kwargs.get('url')
        duration: int = kwargs.get('duration')
        self.decompose(element, self.arrow_selector)
        name: str = self.extract_text(element, self.name_selector)
        study_program: StudyProgram2018 = StudyProgram2018(
            accreditation_year=self.accreditation_year,
            name=name,
            duration=duration,
            url=url,
            title=self.extract_text(element, self.title_selector),
            full_name=f"{name}_{duration}",
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    def parse_data(self, *args, **kwargs) -> list[StudyProgram2018]:
        page_content: str = kwargs.get('page_content')
        url: str = kwargs.get('url')
        soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.main_selector)
        durations: int = len(self.extract_multiple_texts(element, self.duration_selector))
        return [self.parse_row(element=element, url=url, duration=duration) for duration in
                range(self.STUDY_PROGRAM_MAX_DURATION, self.STUDY_PROGRAM_MAX_DURATION - durations, -1)]

