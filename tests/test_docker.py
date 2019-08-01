import asyncio
import os
from pathlib import Path
from typing import Tuple

import docker
import pytest

from golem_task_api import (
    TaskApiService,
    constants as api_constants,
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


class DockerTaskApiService(TaskApiService):
    def __init__(self, work_dir: Path):
        self._work_dir = work_dir
        self._container = None

    def start(self, command: str, port: int) -> Tuple[str, int]:
        self._container = docker.from_env().containers.run(
            TAG,
            command=command,
            volumes={
                str(self._work_dir): {
                    'bind': f'/{api_constants.WORK_DIR}',
                    'mode': 'rw',
                }
            },
            detach=True,
            user=os.getuid(),
        )
        api_client = docker.APIClient()
        c_config = api_client.inspect_container(self._container.id)
        ip_address = \
            c_config['NetworkSettings']['Networks']['bridge']['IPAddress']
        wait_until_socket_open(ip_address, port)
        return ip_address, port

    async def wait_until_shutdown_complete(self) -> None:
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

    def _get_task_api_service(
            self,
            work_dir: Path,
    ) -> TaskApiService:
         return DockerTaskApiService(work_dir)

    @pytest.mark.asyncio
    async def test_requestor_benchmark(self, task_flow_helper):
        async with task_flow_helper.init_requestor(self._get_task_api_service):
            score = await task_flow_helper.requestor_client.run_benchmark()
            assert score > 0

    @pytest.mark.asyncio
    async def test_provider_benchmark(self, task_flow_helper):
        print("init_provider")
        async with task_flow_helper.init_provider(self._get_task_api_service):
            print("await benchmark")
            score = await task_flow_helper.run_provider_benchmark()
            assert score > 0

    @pytest.mark.asyncio
    async def test_provider_shutdown_in_benchmark(self, task_flow_helper):
        async with task_flow_helper.init_provider(self._get_task_api_service):
            benchmark_defer = task_flow_helper.run_provider_benchmark()
            shutdown_defer = task_flow_helper.shutdown_provider()
            done, pending = await asyncio.wait(
                [shutdown_defer, benchmark_defer],
                return_when=asyncio.FIRST_COMPLETED)
            print('done=', done)
            print('pending=', pending)
            assert benchmark_defer.done() == False
