from pathlib import Path
from typing import Tuple
import asyncio
import shutil
import threading

import pytest

from .simulationbase import (
    SimulationBase,
    task_flow_helper,
    wait_until_socket_open,
)

from golem_task_api import (
    AppCallbacks,
)

from golem_blender_app.entrypoint import (
    main,
    ProviderHandler,
    RequestorHandler,
)


class InlineAppCallbacks(AppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._thread = None

    def _spawn(self, command: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main(
            self._work_dir,
            command.split(' '),
            provider_handler=ProviderHandler(),
            requestor_handler=RequestorHandler(),
        ))

    def spawn_server(self, command: str, port: int) -> Tuple[str, int]:
        self._thread = threading.Thread(
            target=self._spawn,
            args=(command,),
            daemon=True,
        )
        self._thread.start()
        host = '127.0.0.1'
        wait_until_socket_open(host, port)
        return host, port

    async def wait_after_shutdown(self) -> None:
        self._thread.join(timeout=3)


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _get_app_callbacks(
            self,
            work_dir: Path,
    ) -> AppCallbacks:
        return InlineAppCallbacks(work_dir)
