import os
from pathlib import Path
from typing import Tuple

import docker
import pytest

from golem_task_api import (
    AppCallbacks,
)

from .simulationbase import (
    SimulationBase,
    task_flow_helper,
    wait_until_socket_open,
)

TAG = 'blenderapp_test'


def is_docker_available():
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


class DockerCallbacks(AppCallbacks):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._container = None

    def spawn_server(self, command: str, port: int) -> Tuple[str, int]:
        volume_name = 'DESKTOP-EHQJOO8/C37A161EDD52B4F2C7C59E6144A47595/test2'
        docker.from_env().volumes.create(
            name=volume_name,
            driver='cifs',
            driver_opts={
                'username': 'golem-docker',
                'password': 'golem-docker'
            }
        )
        self._container = docker.from_env().containers.run(
            TAG,
            command=command,
            volumes={
                volume_name: {'bind': '/golem/work', 'mode': 'rw'}
            },
            detach=True,
            # user=os.getuid(),
            ports={
                port: ('127.0.0.1', None)
            }
        )
        api_client = docker.from_env().api
        c_config = api_client.inspect_container(self._container.id)
        ports = \
            c_config['NetworkSettings']['Ports'][f'{port}/tcp'][0]
        ip_address = ports['HostIp']
        port = int(ports['HostPort'])
        wait_until_socket_open(ip_address, port)
        return ip_address, port

    async def wait_after_shutdown(self) -> None:
        logs = self._container.logs().decode('utf-8')
        print(logs)
        self._container.remove(force=True)


@pytest.mark.skipif(not is_docker_available(), reason='docker not available')
class TestDocker(SimulationBase):

    @classmethod
    def setup_class(cls):
        docker.from_env().images.build(
            path=str(Path(__file__).parent.parent / 'image'),
            tag=TAG,
        )

    def _get_app_callbacks(
            self,
            work_dir: Path,
    ) -> AppCallbacks:
        return DockerCallbacks(work_dir)

    @pytest.mark.asyncio
    async def test_requestor_benchmark(self, task_flow_helper):
        async with task_flow_helper.init_requestor(self._get_app_callbacks):
            score = await task_flow_helper.requestor_client.run_benchmark()
            assert score > 0

    @pytest.mark.asyncio
    async def test_provider_benchmark(self, task_flow_helper):
        task_flow_helper.init_provider(self._get_app_callbacks)
        score = await task_flow_helper.run_provider_benchmark()
        assert score > 0
