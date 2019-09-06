from pathlib import Path
from typing import List, Tuple, Optional
import asyncio
import sys

from golem_task_api import (
    ProviderAppHandler,
    RequestorAppHandler,
    constants as api_constants,
    entrypoint,
    structs,
)

from golem_blender_app.commands.benchmark import benchmark
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.discard_subtasks import discard_subtasks
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.has_pending_subtasks import has_pending_subtasks
from golem_blender_app.commands.verify import verify


class RequestorHandler(RequestorAppHandler):
    async def create_task(
            self,
            task_work_dir: Path,
            max_subtasks_count: int,
            task_params: dict,
    ) -> None:
        create_task(task_work_dir, max_subtasks_count, task_params)

    async def next_subtask(
            self,
            task_work_dir: Path,
    ) -> structs.Subtask:
        return get_next_subtask(task_work_dir)

    async def verify(
            self,
            task_work_dir: Path,
            subtask_id: str,
    ) -> Tuple[bool, Optional[str]]:
        return await verify(task_work_dir, subtask_id)

    async def discard_subtasks(
            self,
            task_work_dir: Path,
            subtask_ids: List[str],
    ) -> List[str]:
        return discard_subtasks(task_work_dir, subtask_ids)

    async def has_pending_subtasks(
            self,
            task_work_dir: Path,
    ) -> bool:
        return has_pending_subtasks(task_work_dir)

    async def run_benchmark(self, work_dir: Path) -> float:
        return await benchmark(work_dir)


class ProviderHandler(ProviderAppHandler):
    async def compute(
            self,
            task_work_dir: Path,
            subtask_id: str,
            subtask_params: dict,
    ) -> Path:
        return await compute(task_work_dir, subtask_id, subtask_params)

    async def run_benchmark(self, work_dir: Path) -> float:
        return await benchmark(work_dir)


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
        provider_handler=provider_handler,
    )


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(
        Path(f'/{api_constants.WORK_DIR}'),
        sys.argv[1:],
        RequestorHandler(),
        ProviderHandler(),
    ))
