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
    TaskApiService,
)

from golem_blender_app.entrypoint import (
    main,
    ProviderHandler,
    RequestorHandler,
)


class InlineTaskApiService(TaskApiService):
    def __init__(self, work_dir: Path):
        # get_child_watcher enables event loops in child threads
        asyncio.get_child_watcher()
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

    def running(self) -> bool:
        return self._thread.is_alive()

    async def start(self, command: str, port: int) -> Tuple[str, int]:
        self._thread = threading.Thread(
            target=self._spawn,
            args=(command,),
            daemon=True,
        )
        self._thread.start()
        host = '127.0.0.1'
        wait_until_socket_open(host, port)
        return host, port

    async def wait_until_shutdown_complete(self) -> None:
        if not self.running():
            print('Service no longer running')
            return
        self._thread.join(timeout=3)


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _get_task_api_service(
            self,
            work_dir: Path,
    ) -> TaskApiService:
        return InlineTaskApiService(work_dir)
