from pathlib import Path
import abc
import asyncio
import json
import random
import shutil
import time

from grpclib.client import Channel

from golem_task_api import constants
from golem_task_api.golem_task_api_pb2 import (
    CreateTaskRequest,
    CreateTaskReply,
    NextSubtaskRequest,
    NextSubtaskReply,
    ComputeRequest,
    ComputeReply,
    VerifyRequest,
    VerifyReply,
)
from golem_task_api.golem_task_api_grpc import (
    GolemAppStub,
)


class SimulationBase(abc.ABC):

    def setup_method(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def teardown_method(self):
        self.loop.stop()
        self.loop.close()

    @abc.abstractmethod
    def _spawn_server(self, work_dir: Path):
        pass

    @abc.abstractmethod
    def _close_server(self, server):
        pass

    def _get_golem_app(self, port: int):
        return GolemAppStub(Channel('127.0.0.1', port, loop=self.loop))

    def _create_task(
            self,
            golem_app,
            task_id: str,
            task_params: dict):
        request = CreateTaskRequest()
        request.task_id = task_id
        request.task_params_json = json.dumps(task_params)
        self.loop.run_until_complete(golem_app.CreateTask(request))

    def _get_next_subtask(self, golem_app, task_id: str):
        request = NextSubtaskRequest()
        request.task_id = task_id
        reply = \
            self.loop.run_until_complete(golem_app.NextSubtask(request))
        return reply.subtask_id, json.loads(reply.subtask_params_json)

    def _compute(
            self,
            golem_app,
            task_id: str,
            subtask_id: str,
            subtask_params: dict):
        request = ComputeRequest()
        request.task_id = task_id
        request.subtask_id = subtask_id
        request.subtask_params_json = json.dumps(subtask_params)
        self.loop.run_until_complete(golem_app.Compute(request))

    def _verify(self, golem_app, task_id: str, subtask_id: str) -> bool:
        request = VerifyRequest()
        request.task_id = task_id
        request.subtask_id = subtask_id
        reply = \
            self.loop.run_until_complete(golem_app.Verify(request))
        return reply.success

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
            subtask_params: dict):
        for resource_id in subtask_params['resources']:
            network_resource = req_net_resources / f'{resource_id}.zip'
            assert network_resource.exists()
            shutil.copy2(network_resource, prov_net_resources)

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
        req_work_dir = tmpdir / 'requestor'
        req_work_dir.mkdir()
        prov_work_dir = tmpdir / 'provider'
        prov_work_dir.mkdir()
        print('tmpdir:', tmpdir)

        requestor_port = 50005
        requestor_server = self._spawn_server(req_work_dir, requestor_port)
        provider_port = 50006
        provider_server = self._spawn_server(prov_work_dir, provider_port)
        try:
            requestor = self._get_golem_app(requestor_port)
            provider = self._get_golem_app(provider_port)
            # Wait for the servers to be ready, I couldn't find a reliable
            # way for that check
            time.sleep(3)

            task_id = 'test_task_id123'
            req_task_work_dir = req_work_dir / task_id
            req_task_work_dir.mkdir()
            req_task_resources_dir = req_task_work_dir / constants.RESOURCES_DIR
            req_task_resources_dir.mkdir()
            req_task_net_resources_dir = \
                req_task_work_dir / constants.NETWORK_RESOURCES_DIR
            req_task_net_resources_dir.mkdir()
            req_task_results_dir = req_task_work_dir / constants.RESULTS_DIR
            req_task_results_dir.mkdir()
            req_task_net_results_dir = \
                req_task_work_dir / constants.NETWORK_RESULTS_DIR
            req_task_net_results_dir.mkdir()

            prov_task_work_dir = prov_work_dir / task_id
            prov_task_work_dir.mkdir()
            prov_task_net_resources_dir = \
                prov_task_work_dir / constants.NETWORK_RESOURCES_DIR
            prov_task_net_resources_dir.mkdir()

            self._put_cube_to_resources(req_task_resources_dir)

            self._create_task(requestor, task_id, task_params)

            for _ in range(task_params['subtasks_count']):

                subtask_id, subtask_params = \
                    self._get_next_subtask(requestor, task_id)
                assert subtask_params['resources'] == [0]

                self._copy_resources_from_requestor(
                    req_task_net_resources_dir,
                    prov_task_net_resources_dir,
                    subtask_params,
                )
                prov_subtask_work_dir = prov_task_work_dir / subtask_id
                prov_subtask_work_dir.mkdir()
                self._compute(provider, task_id, subtask_id, subtask_params)
                self._copy_result_from_provider(
                    prov_subtask_work_dir,
                    req_task_net_results_dir,
                    subtask_id,
                )

                success = self._verify(
                    requestor,
                    task_id,
                    subtask_id,
                )
                assert success

            for frame in expected_frames:
                filename = f'result{frame:04d}.{task_params["format"]}'
                result_file = req_task_results_dir / filename
                assert result_file.exists()
        finally:
            self._close_server(requestor_server)
            self._close_server(provider_server)

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
