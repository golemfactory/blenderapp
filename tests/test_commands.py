from pathlib import Path
import shutil

import pytest

from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.restart_task import restart_task
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.verify import verify

from .simulationbase import SimulationBase


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

    def test_restart(self, tmpdir):
        # Simulate a task, fail before second subtask finished
        task_params = self._get_cube_params(2, "5;9")
        expected_frames = [5, 9]

        tmpdir = Path(tmpdir)
        print('tmpdir:', tmpdir)
        req_work, req_resources, req_net_resources, req_results, \
            req_net_results = self._make_req_dirs(tmpdir)

        self._put_cube_to_resources(req_resources)

        self._dump_task_params(req_work, task_params)
        self._create_task(req_work, req_resources, req_net_resources)

        for _ in range(task_params['subtasks_count']) - 1:
            prov_work, prov_net_resources = self._make_prov_dirs(tmpdir)

            self._get_next_subtask(req_work, req_resources, req_net_resources)
            with open(req_work / 'subtask_id.txt', 'r') as f:
                subtask_id = f.read()
            with open(req_work / f'subtask{subtask_id}.json', 'r') as f:
                subtask_params = json.load(f)
            assert subtask_params['resources'] == [0]

            self._copy_resources_from_requestor(
                req_net_resources,
                prov_net_resources,
                req_work,
                prov_work,
                subtask_id,
                subtask_params,
            )

            self._compute(prov_work, prov_net_resources)
            self._copy_result_from_provider(
                prov_work,
                req_net_results,
                subtask_id,
            )

            self._verify(
                subtask_id,
                req_work,
                req_resources,
                req_net_resources,
                req_results,
                req_net_results,
            )
            with open(req_work / f'verdict{subtask_id}.json', 'r') as f:
                verdict = json.load(f)
            assert verdict == {'verdict': True}

        # restart the task

        old_work = req_work
        old_results = req_results
        old_net_resources = req_net_resources

        req_work, req_resources, req_net_resources, req_results, \
            req_net_results = self._make_req_dirs(tmpdir)

        self._put_cube_to_resources(req_resources)

        self._dump_task_params(req_work, task_params)

        restart_task(
            old_work,
            old_net_resources,
            old_results,
            req_work,
            req_net_resources,
            req_results
        )
        prov_work, prov_net_resources = self._make_prov_dirs(tmpdir)

        self._get_next_subtask(req_work, req_resources, req_net_resources)
        with open(req_work / 'subtask_id.txt', 'r') as f:
            subtask_id = f.read()
        with open(req_work / f'subtask{subtask_id}.json', 'r') as f:
            subtask_params = json.load(f)
        assert subtask_params['resources'] == [0]

        self._copy_resources_from_requestor(
            req_net_resources,
            prov_net_resources,
            req_work,
            prov_work,
            subtask_id,
            subtask_params,
        )

        self._compute(prov_work, prov_net_resources)
        self._copy_result_from_provider(
            prov_work,
            req_net_results,
            subtask_id,
        )

        self._verify(
            subtask_id,
            req_work,
            req_resources,
            req_net_resources,
            req_results,
            req_net_results,
        )
        with open(req_work / f'verdict{subtask_id}.json', 'r') as f:
            verdict = json.load(f)
        assert verdict == {'verdict': True}
        # assert both subtasks are finished now

        for frame in expected_frames:
            result_file = req_results / f'result{frame:04d}.{task_params["format"]}'  # noqa
            assert result_file.exists()
        pass
