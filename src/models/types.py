from src.models.accreditation_2018.data_classes import Curriculum2018, Course2018, StudyProgram2018
from src.models.accreditation_2023.data_classes import StudyProgram2023, Curriculum2023, Course2023

StudyProgram = StudyProgram2018 | StudyProgram2023
Curriculum = Curriculum2018 | Curriculum2023
Course = Course2018 | Course2023
Record = StudyProgram | Curriculum | Course
