from pathlib import Path
from typing import Dict, List, Tuple, Optional
import asyncio
import sys

from golem_task_api import (
    ProviderAppHandler,
    RequestorAppHandler,
    constants as api_constants,
    dirutils,
    entrypoint,
    enums,
    structs,
)
from golem_task_api.dirutils import RequestorTaskDir

from golem_blender_app import commands


class RequestorHandler(RequestorAppHandler):

    def __init__(self) -> None:
        self._running_verifications: Dict[str, asyncio.Future] = {}

    async def create_task(
            self,
            task_work_dir: dirutils.RequestorTaskDir,
            max_subtasks_count: int,
            task_params: dict,
    ) -> structs.Task:
        return await commands.create_task(
            task_work_dir, max_subtasks_count, task_params)

    async def next_subtask(
            self,
            task_work_dir: dirutils.RequestorTaskDir,
            subtask_id: str,
            opaque_node_id: str
    ) -> structs.Subtask:
        return commands.get_next_subtask(task_work_dir, subtask_id)

    async def verify(
            self,
            task_work_dir: dirutils.RequestorTaskDir,
            subtask_id: str,
    ) -> Tuple[enums.VerifyResult, Optional[str]]:
        self._running_verifications[subtask_id] = asyncio.ensure_future(
            commands.verify(task_work_dir, subtask_id))
        try:
            return await self._running_verifications[subtask_id]
        finally:
            del self._running_verifications[subtask_id]

    async def discard_subtasks(
            self,
            task_work_dir: dirutils.RequestorTaskDir,
            subtask_ids: List[str],
    ) -> List[str]:
        return commands.discard_subtasks(task_work_dir, subtask_ids)

    async def abort_task(
            self,
            task_work_dir: dirutils.RequestorTaskDir
    ) -> None:
        commands.abort_task(task_work_dir)
        for verification in self._running_verifications.values():
            verification.cancel()

    async def abort_subtask(
            self,
            task_work_dir: RequestorTaskDir,
            subtask_id: str
    ) -> None:
        commands.abort_subtask(task_work_dir, subtask_id)
        verification = self._running_verifications.get(subtask_id)
        if verification:
            verification.cancel()

    async def has_pending_subtasks(
            self,
            task_work_dir: dirutils.RequestorTaskDir,
    ) -> bool:
        return commands.has_pending_subtasks(task_work_dir)

    async def run_benchmark(self, work_dir: Path) -> float:
        return await commands.benchmark(work_dir)


class ProviderHandler(ProviderAppHandler):
    async def compute(
            self,
            task_work_dir: dirutils.ProviderTaskDir,
            subtask_id: str,
            subtask_params: dict,
    ) -> Path:
        return await commands.compute(task_work_dir, subtask_id, subtask_params)

    async def run_benchmark(self, work_dir: Path) -> float:
        return await commands.benchmark(work_dir)


async def main(
        work_dir: Path,
        argv: List[str],
        requestor_handler: Optional[RequestorHandler] = None,
        provider_handler: Optional[ProviderHandler] = None,
):
    await entrypoint(
        work_dir,
        argv,
        requestor_handler=requestor_handler,
        provider_handler=provider_handler)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(
        Path(f'/{api_constants.WORK_DIR}'),
        sys.argv[1:],
        requestor_handler=RequestorHandler(),
        provider_handler=ProviderHandler(),
    ))
