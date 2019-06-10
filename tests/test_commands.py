from pathlib import Path
from typing import Tuple
import asyncio
import shutil

import pytest

from .simulationbase import SimulationBase

from golem_task_api import (
    ProviderAppCallbacks,
    RequestorAppCallbacks,
)

from golem_blender_app.entrypoint import (
    main,
    ProviderHandler,
    RequestorHandler,
)


class InlineRequestorCallbacks(RequestorAppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._task = None

    def spawn_server(self, command: str, port: int) -> Tuple[str, int]:
        self._task = asyncio.get_event_loop().create_task(main(
            self._work_dir,
            command.split(' '),
            requestor_handler=RequestorHandler(),
        ))
        return '127.0.0.1', port

    async def wait_after_shutdown(self) -> None:
        await self._task


class InlineProviderCallbacks(ProviderAppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir

    async def run_command(self, command: str) -> None:
        await main(
            self._work_dir,
            command.split(' '),
            provider_handler=ProviderHandler(),
        )


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _get_requestor_app_callbacks(
            self,
            work_dir: Path,
    ) -> RequestorAppCallbacks:
        return InlineRequestorCallbacks(work_dir)

    def _get_provider_app_callbacks(
            self,
            work_dir: Path,
    ) -> ProviderAppCallbacks:
        return InlineProviderCallbacks(work_dir)
