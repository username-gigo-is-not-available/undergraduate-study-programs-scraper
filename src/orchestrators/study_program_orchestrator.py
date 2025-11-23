import asyncio
from ssl import SSLContext

from aiohttp import ClientSession

from src.models.data_classes import StudyProgram
from src.models.types import Record
from src.network import HTTPClient
from src.orchestrators.base_orchestrator import BaseOrchestrator


class StudyProgramOrchestrator(BaseOrchestrator):

    def create_tasks(self,session: ClientSession,
                  ssl_context: SSLContext,
                  page_content: str,
                  http_client: HTTPClient,
                  semaphore: asyncio.Semaphore) -> list[asyncio.Task[Record]]:
        tasks: list[asyncio.Task[list[StudyProgram]]] = []
        for parser in self.parsers:
            tasks.append(asyncio.create_task(
                parser.run(
                    session=session,
                    ssl_context=ssl_context,
                    page_content=page_content,
                    http_client=http_client,
                    semaphore=semaphore
                )
            )
            )
        return tasks

