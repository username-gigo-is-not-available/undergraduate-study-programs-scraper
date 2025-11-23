import logging

from bs4 import Tag, BeautifulSoup

from src.models.data_classes import StudyProgram
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


    def parse_row(self, *args, **kwargs) -> StudyProgram:
        element: Tag = kwargs.get('element')
        url: str = kwargs.get('url')
        duration: int = kwargs.get('duration')
        title: str = kwargs.get('title')
        self.decompose(element, self.arrow_selector)
        study_program: StudyProgram = StudyProgram(
            accreditation_year=self.accreditation_year,
            name=self.extract_text(element, self.name_selector),
            duration=duration,
            url=url,
            title=title
        )
        logging.info(f"Scraped study_program {study_program}")
        return study_program

    def parse_data(self, *args, **kwargs) -> list[StudyProgram]:
        page_content: str = kwargs.get('page_content')
        url: str = kwargs.get('url')
        soup: BeautifulSoup = BaseParser.get_parsed_html(page_content)
        element: Tag = soup.select_one(self.main_selector)
        number_of_durations: int = len(self.extract_multiple_texts(element, self.duration_selector))
        titles: list[str] = self.extract_texts_from_node(element, self.title_selector)
        study_programs: list[StudyProgram] = []
        for duration, title in zip(range(self.STUDY_PROGRAM_MAX_DURATION, self.STUDY_PROGRAM_MAX_DURATION - number_of_durations, -1), titles[::-1]):
            study_programs.append(self.parse_row(element=element, url=url, duration=duration, title=title))

        return study_programs

