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
    def _restart_task(
            self,
            req_work: Path,
            req_results: Path):
        self._run_command(
            'restart-task',
            work=req_work,
            results=req_results
        )

    def test_one_subtasks_one_frame(self, tmpdir):
        self._simulate(self._get_cube_params(1, "1"), tmpdir, [1])

    def test_one_subtasks_three_frames(self, tmpdir):
        self._simulate(self._get_cube_params(1, "2-3;8"), tmpdir, [2, 3, 8])

    def test_two_subtasks_one_frame(self, tmpdir):
        self._simulate(self._get_cube_params(2, "5"), tmpdir, [5])

    def test_two_subtasks_two_frames(self, tmpdir):
        self._simulate(self._get_cube_params(2, "5;9"), tmpdir, [5, 9])

    def test_four_subtasks_two_frames(self, tmpdir):
        self._simulate(self._get_cube_params(4, "6-7"), tmpdir, [6, 7])

    def test_restart(self, tmpdir):
        self._simulate_restart(self._get_cube_params(2, "5;9"), tmpdir, [5, 9])
