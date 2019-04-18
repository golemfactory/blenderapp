from pathlib import Path
import shutil

import pytest

from image.entrypoints.scripts.commands.create_task import create_task
from image.entrypoints.scripts.commands.get_subtask import get_next_subtask
from image.entrypoints.scripts.commands.compute import compute
from image.entrypoints.scripts.commands.verify import verify

from simulationbase import SimulationBase


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _create_task(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        create_task(req_work, req_resources, req_net_resources)

    def _get_next_subtask(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        get_next_subtask(req_work, req_resources, req_net_resources)

    def _compute(
            self,
            prov_work: Path,
            prov_net_resources: Path):
        compute(prov_work, prov_net_resources)

    def _verify(
            self,
            subtask_id: str,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path,
            req_results: Path,
            req_net_results: Path):
        verify(
            subtask_id,
            req_work,
            req_resources,
            req_net_resources,
            req_results,
            req_net_results,
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
