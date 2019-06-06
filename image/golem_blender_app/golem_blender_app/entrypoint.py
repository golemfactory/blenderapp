from pathlib import Path
from typing import List, Optional, Tuple
import asyncio
import sys

from golem_task_api import (
    ProviderAppHandler,
    RequestorAppHandler,
    entrypoint,
)

from golem_blender_app.commands.benchmark import benchmark
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.verify import verify


class RequestorHandler(RequestorAppHandler):
    async def create_task(
            self,
            task_work_dir: Path,
            task_params: dict) -> None:
        create_task(task_work_dir, task_params)

    async def next_subtask(
            self,
            task_work_dir: Path) -> Tuple[str, dict]:
        return get_next_subtask(task_work_dir)

    async def verify(
            self,
            task_work_dir: Path,
            subtask_id: str) -> bool:
        return verify(task_work_dir, subtask_id)

    async def run_benchmark(self, work_dir: Path) -> float:
        return benchmark(work_dir)


class ProviderHandler(ProviderAppHandler):
    async def compute(
            self,
            task_work_dir: Path,
            subtask_id: str,
            subtask_params: dict) -> None:
        compute(task_work_dir, subtask_id, subtask_params)

    async def run_benchmark(self, work_dir: Path) -> float:
        return benchmark(work_dir)


async def main(
        work_dir: Path,
        argv: List[str],
        requestor_handler: Optional[RequestorHandler] = None,
        provider_handler: Optional[ProviderHandler] = None,
):
    await entrypoint(work_dir, argv, requestor_handler, provider_handler)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(
        Path('/golem/work'),
        sys.argv[1:],
        RequestorHandler(),
        ProviderHandler(),
    ))
