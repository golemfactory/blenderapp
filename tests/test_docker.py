import asyncio
import os
from pathlib import Path
from typing import Tuple

import docker
import pytest

from golem_task_api import (
    ProviderAppCallbacks,
    RequestorAppCallbacks,
    RequestorAppClient,
)

from .simulationbase import SimulationBase

TAG = 'blenderapp_test'
NAME = 'blenderapp_test_container'


def is_docker_available():
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


class DockerRequestorCallbacks(RequestorAppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._task = None

    def spawn_server(self, command: str, port: int) -> Tuple[str, int]:
        c = docker.from_env().containers.run(
            TAG,
            command=command,
            volumes={
                str(self._work_dir): {'bind': '/golem/work', 'mode': 'rw'}
            },
            detach=True,
            user=os.getuid(),
            name=NAME,
        )
        api_client = docker.APIClient()
        c_config = api_client.inspect_container(c.id)
        ip_address = \
            c_config['NetworkSettings']['Networks']['bridge']['IPAddress']
        return ip_address, port

    async def wait_after_shutdown(self) -> None:
        pass


class DockerProviderCallbacks(ProviderAppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir

    async def run_command(self, command: str) -> None:
        docker.from_env().containers.run(
            TAG,
            command=command,
            volumes={
                str(self._work_dir): {'bind': '/golem/work', 'mode': 'rw'}
            },
            user=os.getuid(),
        )


@pytest.mark.skipif(not is_docker_available(), reason='docker not available')
class TestDocker(SimulationBase):

    def teardown_method(self):
        c = docker.from_env().containers.get(NAME)
        if c:
            logs = c.logs().decode('utf-8')
            print(logs)
            c.kill()
            c.remove()

    @classmethod
    def setup_class(cls):
        docker.from_env().images.build(
            path=str(Path(__file__).parent.parent / 'image'),
            tag=TAG,
        )

    def _get_requestor_app_callbacks(
            self,
            work_dir: Path,
    ) -> RequestorAppCallbacks:
        return DockerRequestorCallbacks(work_dir)

    def _get_provider_app_callbacks(
            self,
            work_dir: Path,
    ) -> ProviderAppCallbacks:
        return DockerProviderCallbacks(work_dir)

    @pytest.mark.asyncio
    async def test_benchmark(self, tmpdir):
        print(tmpdir)
        work_dir = Path(tmpdir)

        port = 50005
        client_callbacks = self._get_requestor_app_callbacks(work_dir)
        client = RequestorAppClient(
            client_callbacks,
            port,
        )
        try:
            await asyncio.sleep(3)
            score = await client.run_benchmark()
            assert score > 0
        finally:
            await client.shutdown()
            await client_callbacks.wait_after_shutdown()
