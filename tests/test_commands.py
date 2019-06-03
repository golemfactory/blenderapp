from pathlib import Path
import shutil

import pytest

from .simulationbase import SimulationBase

from golem_blender_app.entrypoint import spawn_server


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    async def _spawn_server(self, work_dir: Path, port: int):
        return await spawn_server(work_dir, port)

    async def _close_server(self, server):
        await server.stop()
