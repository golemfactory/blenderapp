from pathlib import Path
import abc
import json
import random
import shutil


class SimulationBase(abc.ABC):
    @abc.abstractmethod
    def _create_task(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        pass

    @abc.abstractmethod
    def _get_next_subtask(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        pass

    @abc.abstractmethod
    def _compute(
            self,
            prov_work: Path,
            prov_net_resources: Path):
        pass

    @abc.abstractmethod
    def _verify(
            self,
            subtask_id: str,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path,
            req_results: Path,
            req_net_results: Path):
        pass

    @abc.abstractmethod
    def _copy_task(
            self,
            req_work: Path,
            req_results: Path):
        pass

    def _make_req_dirs(self, tmpdir):
        req = tmpdir / f'req{random.random()}'
        req_work = req / 'work'
        req_resources = req / 'resources'
        req_net_resources = req / 'network_resources'
        req_results = req / 'results'
        req_net_results = req / 'network_results'
        for p in [req, req_work, req_resources, req_net_resources, req_results,
                  req_net_results]:
            p.mkdir()
        return req_work, req_resources, req_net_resources, req_results, \
            req_net_results

    def _make_prov_dirs(self, tmpdir):
        prov = tmpdir / f'prov{random.random()}'
        prov_work = prov / 'work'
        prov_net_resources = prov / 'network_resources'
        for p in [prov, prov_work, prov_net_resources]:
            p.mkdir()
        return prov_work, prov_net_resources

    @staticmethod
    def _put_cube_to_resources(req_resources: Path):
        shutil.copy2(
            Path('.') / 'image' / 'benchmark'/ 'cube.blend',  # noqa
            req_resources,
        )

    @staticmethod
    def _dump_task_params(req_work: Path, task_params: dict):
        with open(req_work / 'task_params.json', 'w') as f:
            json.dump(task_params, f)

    @staticmethod
    def _copy_resources_from_requestor(
            req_net_resources: Path,
            prov_net_resources: Path,
            req_work: Path,
            prov_work: Path,
            subtask_id: str,
            subtask_params: dict):
        for resource_id in subtask_params['resources']:
            network_resource = req_net_resources / f'{resource_id}.zip'
            assert network_resource.exists()
            shutil.copy2(network_resource, prov_net_resources)
        shutil.copy2(
            req_work / f'subtask{subtask_id}.json',
            prov_work / 'params.json',
        )

    @staticmethod
    def _copy_result_from_provider(
            prov_work: Path,
            req_net_results: Path,
            subtask_id: str):
        result = prov_work / 'result.zip'
        assert result.exists()
        shutil.copy2(
            result,
            req_net_results / f'{subtask_id}.zip',
        )

    @staticmethod
    def _get_cube_params(
            subtasks_count: int,
            frames: str,
            output_format: str="png"):
        return {
            "subtasks_count": subtasks_count,
            "format": output_format,
            "resolution": [1000, 600],
            "frames": frames,
            "scene_file": "cube.blend",
            "resources": [
                "cube.blend",
            ]
        }

    def _simulate(self, task_params: dict, tmpdir, expected_frames: list):
        tmpdir = Path(tmpdir)
        print('tmpdir:', tmpdir)
        req_work, req_resources, req_net_resources, req_results, \
            req_net_results = self._make_req_dirs(tmpdir)

        self._put_cube_to_resources(req_resources)

        self._dump_task_params(req_work, task_params)
        self._create_task(req_work, req_resources, req_net_resources)

        for _ in range(task_params['subtasks_count']):
            self._do_subtask(tmpdir, req_work, req_resources, req_results, req_net_resources, req_net_results)

        for frame in expected_frames:
            result_file = \
                req_results / f'result{frame:04d}.{task_params["format"]}'
            assert result_file.exists()

    def _simulate_restart(self, task_params: dict, tmpdir, expected_frames: list, subtasks_to_skip: int = 1):
        cur_tmpdir = Path(tmpdir) / 'failed_task'
        cur_tmpdir.mkdir()
        print('tmpdir:', cur_tmpdir)
        req_work, req_resources, req_net_resources, req_results, \
            req_net_results = self._make_req_dirs(cur_tmpdir)

        self._put_cube_to_resources(req_resources)

        self._dump_task_params(req_work, task_params)
        self._create_task(req_work, req_resources, req_net_resources)

        for _ in range(task_params['subtasks_count'] - subtasks_to_skip):
            self._do_subtask(cur_tmpdir, req_work, req_resources, req_results, req_net_resources, req_net_results)

        # restart the task

        old_work = req_work
        old_results = req_results
        old_net_resources = req_net_resources

        cur_tmpdir = Path(tmpdir) / 'restarted_task'
        cur_tmpdir.mkdir()
        print('tmpdir:', cur_tmpdir)
        req_work, req_resources, req_net_resources, req_results, \
            req_net_results = self._make_req_dirs(cur_tmpdir)

        self._put_cube_to_resources(req_resources)
        req_net_resources.rmdir()
        shutil.copytree(old_net_resources, req_net_resources)
        shutil.copytree(old_work, req_work / 'restarted')
        shutil.copytree(old_results, req_results / 'restarted')

        self._dump_task_params(req_work, task_params)
        self._copy_task(
            req_work,
            req_results
        )
        for _ in range(subtasks_to_skip):
            self._do_subtask(cur_tmpdir, req_work, req_resources, req_results, req_net_resources, req_net_results)

        # assert both subtasks are finished now

        for frame in expected_frames:
            result_file = \
                req_results / f'result{frame:04d}.{task_params["format"]}'
            assert result_file.exists()

    def _do_subtask(self, tmpdir, req_work, req_resources, req_results, req_net_resources, req_net_results):
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

    def test_one_subtasks_one_frame(self, tmpdir):
        self._simulate(self._get_cube_params(1, "1"), tmpdir, [1])

    def test_one_subtasks_three_frames(self, tmpdir):
        self._simulate(self._get_cube_params(1, "2-3;8"), tmpdir, [2, 3, 8])

    def test_two_subtasks_one_frame_png(self, tmpdir):
        self._simulate(self._get_cube_params(2, "5"), tmpdir, [5])

    def test_two_subtasks_one_frame_exr(self, tmpdir):
        self._simulate(self._get_cube_params(2, "5", "exr"), tmpdir, [5])

    def test_two_subtasks_two_frames(self, tmpdir):
        self._simulate(self._get_cube_params(2, "5;9"), tmpdir, [5, 9])

    def test_four_subtasks_two_frames(self, tmpdir):
        self._simulate(self._get_cube_params(4, "6-7"), tmpdir, [6, 7])

    def test_restart(self, tmpdir):
        self._simulate_restart(self._get_cube_params(2, "5;9"), tmpdir, [5, 9])
