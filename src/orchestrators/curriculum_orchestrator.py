import asyncio
from concurrent.futures import Executor

from src.models.data_classes import Curriculum
from src.models.types import Record
from src.orchestrators.base_orchestrator import BaseOrchestrator
from src.orchestrators.study_program_orchestrator import StudyProgramOrchestrator
from src.parsers.base_parser import BaseParser


class CurriculumOrchestrator(BaseOrchestrator):

    def create_tasks(self,
                      study_program_orchestrator: StudyProgramOrchestrator,
                      executor: Executor) -> list[asyncio.Task[Record]]:
        tasks: list[asyncio.Task[list[Curriculum]]] = []
        for study_program_parser, curriculum_parser in zip(study_program_orchestrator.parsers, self.parsers):
            tasks.append(asyncio.create_task(
                curriculum_parser.run(
                    study_program_parser=study_program_parser,
                    executor=executor,
                )
            )
            )
        return tasks

