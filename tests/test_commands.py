from pathlib import Path
import asyncio
import shutil

import pytest

from .simulationbase import SimulationBase

from golem_blender_app.entrypoint import spawn_server


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _spawn_server(self, work_dir: Path, port: int):
        return spawn_server(work_dir, port)

    def _close_server(self, server):
        server.close()
        asyncio.get_event_loop().run_until_complete(server.wait_closed())
