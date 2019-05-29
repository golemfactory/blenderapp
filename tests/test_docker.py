import os
import time
from pathlib import Path

import docker
import pytest

from .simulationbase import SimulationBase

from golem_task_api.golem_task_api_pb2 import (
    RunBenchmarkRequest,
)

TAG = 'blenderapp_test'


def is_docker_available():
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


@pytest.mark.skipif(not is_docker_available(), reason='docker not available')
class TestDocker(SimulationBase):

    @classmethod
    def setup_class(cls):
        cls.client = docker.from_env()
        cls.client.images.build(
            path=str(Path(__file__).parent.parent / 'image'),
            tag=TAG,
        )

    def _spawn_server(self, work_dir: Path, port: int):
        return self.client.containers.run(
            TAG,
            command=str(port),
            volumes={
                str(work_dir): {'bind': '/golem/work', 'mode': 'rw'}
            },
            detach=True,
            ports={
                port: ('127.0.0.1', port),
            },
            user=os.getuid(),
        )

    def _close_server(self, server):
        logs = server.logs().decode('utf-8')
        print(logs)
        server.kill()

    def test_benchmark(self, tmpdir):
        port = 50005
        server = self._spawn_server(Path(tmpdir), port)
        golem_app = self._get_golem_app(port)
        time.sleep(3)

        request = RunBenchmarkRequest()
        reply = self.loop.run_until_complete(golem_app.RunBenchmark(request))
        assert reply.score > 0

        self._close_server(server)
