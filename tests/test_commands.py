from pathlib import Path
import shutil

import pytest

from .simulationbase import SimulationBase

from golem_blender_app.entrypoint import (
    spawn_provider_server,
    spawn_requestor_server,
)


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    async def _spawn_requestor_server(self, work_dir: Path, port: int):
        return await spawn_requestor_server(work_dir, port)

    async def _spawn_provider_server(self, work_dir: Path, port: int):
        return await spawn_provider_server(work_dir, port)

    async def _close_server(self, server):
        await server.stop()
