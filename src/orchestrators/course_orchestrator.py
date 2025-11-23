import asyncio
from concurrent.futures import Executor
from ssl import SSLContext

from aiohttp import ClientSession

from src.models.data_classes import Course
from src.models.types import Record
from src.network import HTTPClient
from src.orchestrators.base_orchestrator import BaseOrchestrator
from src.orchestrators.curriculum_orchestrator import CurriculumOrchestrator


class CourseOrchestrator(BaseOrchestrator):


    def create_tasks(self,
            curriculum_orchestrator: CurriculumOrchestrator,
            session: ClientSession,
            ssl_context: SSLContext,
            http_client: HTTPClient,
            semaphore: asyncio.Semaphore,
            executor: Executor) -> list[asyncio.Task[Record]]:

        tasks: list[asyncio.Task[list[Course]]] = []
        for curriculum_parser, course_parser in zip(curriculum_orchestrator.parsers, self.parsers):
            tasks.append(asyncio.create_task(
                course_parser.run(
                    session=session,
                    ssl_context=ssl_context,
                    http_client=http_client,
                    curriculum_parser=curriculum_parser,
                    semaphore=semaphore,
                    executor=executor,
                )
            )
            )
        return tasks
