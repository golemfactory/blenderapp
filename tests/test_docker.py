import os
from pathlib import Path
from sys import platform
from typing import Dict, Optional, Tuple

import docker
from docker.errors import APIError
from docker.models.containers import Container
import pytest

from golem_task_api import (
    TaskApiService,
    constants as api_constants,
)

from .simulationbase import SimulationBase

TAG = 'blenderapp_test'


def is_docker_available():
    try:
        docker.from_env().ping()
    except APIError:
        return False
    return True


class DockerTaskApiService(TaskApiService):
    def __init__(self, work_dir: Path):
        self._work_dir: Path = work_dir
        self._container: Optional[Container] = None

    async def start(self, command: str, port: int) -> Tuple[str, int]:
        ports: Dict[int, int] = {}
        if platform == 'darwin':
            ports = {port: port}
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
            ports=ports
        )
        api_client = docker.APIClient()
        c_config = api_client.inspect_container(self._container.id)
        if platform == 'darwin':
            ip_address = '127.0.0.1'
        else:
            ip_address = \
                c_config['NetworkSettings']['Networks']['bridge']['IPAddress']
        return ip_address, port

    async def stop(self) -> None:
        pass

    def running(self) -> bool:
        if not self._container:
            return False
        try:
            self._container.reload()
        except docker.errors.NotFound:
            return False
        print('Check container status', self._container.status)
        return self._container.status not in ['exited', 'error']

    async def wait_until_shutdown_complete(self) -> None:
        if not (self._container and self.running()):
            return
        print('Shutting down container with status: ', self._container.status)
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
    async def test_requestor_benchmark(self, task_lifecycle_util):
        async with task_lifecycle_util.init_requestor(
                self._get_task_api_service):
            score = await task_lifecycle_util.requestor_client.run_benchmark()
            assert score > 0

    @pytest.mark.asyncio
    async def test_provider_benchmark(self, task_lifecycle_util):
        print("init_provider")
        task_id = 'test-task-id-123'
        task_lifecycle_util.init_provider(self._get_task_api_service, task_id)
        await task_lifecycle_util.start_provider()
        print("await benchmark")
        score = await task_lifecycle_util.provider_client.run_benchmark()
        assert score > 0
