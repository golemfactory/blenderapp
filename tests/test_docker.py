from pathlib import Path
from typing import Optional

import docker
import pytest

from .simulationbase import SimulationBase

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

    def _run_command(
            self,
            command: str,
            work: Optional[Path]=None,
            resources: Optional[Path]=None,
            network_resources: Optional[Path]=None,
            results: Optional[Path]=None,
            network_results: Optional[Path]=None):
        volumes = {}
        if work:
            volumes[work] = {'bind': '/golem/work', 'mode': 'rw'}
        if resources:
            volumes[resources] = {'bind': '/golem/resources', 'mode': 'rw'}
        if network_resources:
            volumes[network_resources] = \
                {'bind': '/golem/network_resources', 'mode': 'rw'}
        if results:
            volumes[results] = {'bind': '/golem/results', 'mode': 'rw'}
        if network_results:
            volumes[network_results] = \
                {'bind': '/golem/network_results', 'mode': 'rw'}
        self.client.containers.run(
            TAG,
            command,
            volumes=volumes,
        )

    def test_benchmark(self):
        self._run_command('benchmark')

    def _create_task(
            self,
            work: Path,
            resources: Path,
            network_resources: Path):
        self._run_command(
            'create-task',
            work=work,
            resources=resources,
            network_resources=network_resources,
        )

    def _get_next_subtask(
            self,
            work: Path,
            resources: Path,
            network_resources: Path):
        self._run_command(
            'get-next-subtask',
            work=work,
            resources=resources,
            network_resources=network_resources,
        )

    def _compute(
            self,
            work: Path,
            network_resources: Path):
        self._run_command(
            'compute',
            work=work,
            network_resources=network_resources,
        )

    def _verify(
            self,
            subtask_id: str,
            work: Path,
            resources: Path,
            network_resources: Path,
            results: Path,
            network_results: Path):
        self._run_command(
            f'verify {subtask_id}',
            work=work,
            resources=resources,
            network_resources=network_resources,
            results=results,
            network_results=network_results,
        )
    def _copy_task(
            self,
            req_work: Path,
            req_results: Path):
        self._run_command(
            'copy-task',
            work=req_work,
            results=req_results
        )
