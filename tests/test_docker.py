import os
from pathlib import Path
from typing import Tuple

import docker
import pytest

from golem_task_api import (
    ProviderAppCallbacks,
    RequestorAppCallbacks,
)

from .simulationbase import (
    ExtendedRequestorAppCallbacks,
    SimulationBase,
    task_flow_helper,
)

TAG = 'blenderapp_test'


def is_docker_available():
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


class DockerRequestorCallbacks(ExtendedRequestorAppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._container = None

    def spawn_server(self, command: str, port: int) -> Tuple[str, int]:
        self._container = docker.from_env().containers.run(
            TAG,
            command=command,
            volumes={
                str(self._work_dir): {'bind': '/golem/work', 'mode': 'rw'}
            },
            detach=True,
            user=os.getuid(),
        )
        api_client = docker.APIClient()
        c_config = api_client.inspect_container(self._container.id)
        ip_address = \
            c_config['NetworkSettings']['Networks']['bridge']['IPAddress']
        return ip_address, port

    async def wait_after_shutdown(self) -> None:
        logs = self._container.logs().decode('utf-8')
        print(logs)
        self._container.remove(force=True)


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

    @classmethod
    def setup_class(cls):
        docker.from_env().images.build(
            path=str(Path(__file__).parent.parent / 'image'),
            tag=TAG,
        )

    def _get_requestor_app_callbacks(
            self,
            work_dir: Path,
    ) -> ExtendedRequestorAppCallbacks:
        return DockerRequestorCallbacks(work_dir)

    def _get_provider_app_callbacks(
            self,
            work_dir: Path,
    ) -> ProviderAppCallbacks:
        return DockerProviderCallbacks(work_dir)

    @pytest.mark.asyncio
    async def test_requestor_benchmark(self, task_flow_helper):
        async with task_flow_helper.init_requestor(
                self._get_requestor_app_callbacks):
            score = await task_flow_helper.requestor_client.run_benchmark()
            assert score > 0

    @pytest.mark.asyncio
    async def test_provider_benchmark(self, task_flow_helper):
        task_flow_helper.init_provider(self._get_provider_app_callbacks)
        score = await task_flow_helper.run_provider_benchmark()
        assert score > 0
