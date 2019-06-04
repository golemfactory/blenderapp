from pathlib import Path
from typing import Tuple
import asyncio
import sys

from golem_task_api import (
    ProviderGolemAppHandler,
    ProviderGolemAppServer,
    RequestorGolemAppHandler,
    RequestorGolemAppServer,
)

from golem_blender_app.commands.benchmark import benchmark
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.verify import verify


class RequestorHandler(RequestorGolemAppHandler):
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


class ProviderHandler(ProviderGolemAppHandler):
    async def compute(
            self,
            task_work_dir: Path,
            subtask_id: str,
            subtask_params: dict) -> None:
        compute(task_work_dir, subtask_id, subtask_params)

    async def run_benchmark(self, work_dir: Path) -> float:
        return benchmark(work_dir)


async def spawn_requestor_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    handler = RequestorHandler()
    server = RequestorGolemAppServer(work_dir, port, handler)
    await server.start()
    return server


async def run_requestor_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    server = await spawn_requestor_server(work_dir, port)
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        print('Shutting down server...')
        await server.stop()


async def spawn_provider_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    handler = ProviderHandler()
    server = ProviderGolemAppServer(work_dir, port, handler)
    await server.start()
    return server


async def run_provider_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    server = await spawn_provider_server(work_dir, port)
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        print('Shutting down server...')
        await server.stop()


if __name__ == '__main__':
    if sys.argv[1] == 'requestor':
        asyncio.get_event_loop().run_until_complete(
            run_requestor_server(Path('/golem/work'), int(sys.argv[2])))
    elif sys.argv[1] == 'provider':
        asyncio.get_event_loop().run_until_complete(
            run_provider_server(Path('/golem/work'), int(sys.argv[2])))
    else:
        raise Exception(f'Unknown command: {sys.argv[1]}')
