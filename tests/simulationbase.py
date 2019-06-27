from async_generator import asynccontextmanager
from pathlib import Path
from typing import Callable, List, Tuple
import abc
import contextlib
import shutil
import pytest
import socket
import time

from golem_task_api import (
    AppCallbacks,
    constants,
    ProviderAppClient,
    RequestorAppClient,
)


def wait_until_socket_open(host: str, port: int, timeout: float = 3.0) -> None:
    time.sleep(3)
    return
    deadline = time.time() + timeout
    while time.time() < deadline:
        with contextlib.closing(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.05)
    raise Exception(f'Could not connect to socket ({host}, {port})')


class TaskFlowHelper:
    def __init__(self, work_dir: Path) -> None:
        print('workdir:', work_dir)
        work_dir = Path('c:\\users\\golem\\appdata\\local\\golem\\golem\\default\\rinkeby\\computerres\\test3')
        self.work_dir = work_dir
        self.req_work_dir = work_dir / 'requestor'
        shutil.rmtree(self.req_work_dir, ignore_errors=True)
        self.req_work_dir.mkdir()
        self.prov_work_dir = work_dir / 'provider'
        shutil.rmtree(self.prov_work_dir, ignore_errors=True)
        self.prov_work_dir.mkdir()

    def init_provider(
            self,
            get_app_callbacks: Callable[[Path], AppCallbacks],
    ) -> None:
        self.get_provider_app_callbacks = get_app_callbacks

    @asynccontextmanager
    async def init_requestor(
            self,
            get_app_callbacks: Callable[[Path], AppCallbacks],
            port: int = 50005,
    ):
        app_callbacks = get_app_callbacks(self.req_work_dir)
        self.requestor_client = RequestorAppClient(app_callbacks, port)
        try:
            yield self.requestor_client
        finally:
            await self.requestor_client.shutdown()

    def mkdir_requestor(self, task_id: str) -> None:
        self.req_task_work_dir = self.req_work_dir / task_id
        self.req_task_work_dir.mkdir()
        self.req_task_resources_dir = \
            self.req_task_work_dir / constants.RESOURCES_DIR
        self.req_task_resources_dir.mkdir()
        self.req_task_net_resources_dir = \
            self.req_task_work_dir / constants.NETWORK_RESOURCES_DIR
        self.req_task_net_resources_dir.mkdir()
        self.req_task_results_dir = \
            self.req_task_work_dir / constants.RESULTS_DIR
        self.req_task_results_dir.mkdir()
        self.req_task_net_results_dir = \
            self.req_task_work_dir / constants.NETWORK_RESULTS_DIR
        self.req_task_net_results_dir.mkdir()

    def mkdir_provider_task(self, task_id: str) -> None:
        self.prov_task_work_dir = self.prov_work_dir / task_id
        self.prov_task_work_dir.mkdir()
        self.prov_task_net_resources_dir = \
            self.prov_task_work_dir / constants.NETWORK_RESOURCES_DIR
        self.prov_task_net_resources_dir.mkdir()

    def mkdir_provider_subtask(self, subtask_id: str) -> None:
        prov_subtask_work_dir = self.prov_task_work_dir / subtask_id
        prov_subtask_work_dir.mkdir()

    def put_cube_to_resources(self) -> None:
        shutil.copy2(
            Path('.') / 'image' / 'benchmark' / 'cube.blend',
            self.req_task_resources_dir,
        )

    def copy_resources_from_requestor(self, resources: List[str]) -> None:
        for resource_id in resources:
            network_resource = self.req_task_net_resources_dir / resource_id
            assert network_resource.exists()
            shutil.copy2(network_resource, self.prov_task_net_resources_dir)

    def copy_result_from_provider(
            self,
            output_filepath: Path,
            subtask_id: str,
    ) -> None:
        result = self.prov_task_work_dir / output_filepath
        assert result.exists()
        shutil.copy2(
            result,
            self.req_task_net_results_dir / f'{result.name}',
        )

    async def create_cube_task(self, task_id: str, task_params: dict) -> None:
        self.mkdir_requestor(task_id)
        self.mkdir_provider_task(task_id)

        self.put_cube_to_resources()

        await self.requestor_client.create_task(task_id, task_params)

    async def compute_remaining_subtasks(self, task_id: str) -> List[str]:
        """ Returns list of subtask IDs """
        subtask_ids = []
        while await self.requestor_client.has_pending_subtasks(task_id):
            subtask_id, verdict = await self.compute_next_subtask(task_id)
            assert verdict
            subtask_ids.append(subtask_id)
        return subtask_ids

    async def compute_next_subtask(self, task_id: str) -> Tuple[str, bool]:
        """ Returns (subtask_id, verification result) """
        assert await self.requestor_client.has_pending_subtasks(task_id)
        subtask = await self.requestor_client.next_subtask(task_id)
        assert subtask.resources == ['0.zip']

        self.copy_resources_from_requestor(subtask.resources)
        self.mkdir_provider_subtask(subtask.subtask_id)

        output_filepath = await ProviderAppClient.compute(
            self.get_provider_app_callbacks(self.prov_task_work_dir),
            task_id,
            subtask.subtask_id,
            subtask.params,
        )
        self.copy_result_from_provider(output_filepath, subtask.subtask_id)

        verdict = \
            await self.requestor_client.verify(task_id, subtask.subtask_id)
        return (subtask.subtask_id, verdict)

    async def run_provider_benchmark(self) -> float:
        return await ProviderAppClient.run_benchmark(
            self.get_provider_app_callbacks(self.prov_work_dir),
        )

    def check_results(
            self,
            output_format: str,
            expected_frames: List[int],
    ) -> None:
        for frame in expected_frames:
            filename = f'result{frame:04d}.{output_format}'
            result_file = self.req_task_results_dir / filename
            assert result_file.exists()


@pytest.fixture
def task_flow_helper(tmpdir):
    return TaskFlowHelper(Path(tmpdir))


class SimulationBase(abc.ABC):

    @abc.abstractmethod
    def _get_app_callbacks(
            self,
            work_dir: Path,
    ) -> AppCallbacks:
        pass

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

    async def _simulate(
            self,
            task_params: dict,
            task_flow_helper: TaskFlowHelper,
            expected_frames: list,
    ):
        task_flow_helper.init_provider(self._get_app_callbacks)
        async with task_flow_helper.init_requestor(
                self._get_app_callbacks) as requestor_client:
            task_id = 'test_task_id123'
            await task_flow_helper.create_cube_task(task_id, task_params)

            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            assert len(subtask_ids) <= task_params['subtasks_count']

            assert not await requestor_client.has_pending_subtasks(task_id)
            task_flow_helper.check_results(
                task_params["format"],
                expected_frames,
            )

    @pytest.mark.asyncio
    async def test_one_subtasks_one_frame(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(1, "1"),
            task_flow_helper,
            [1],
        )

    @pytest.mark.asyncio
    async def test_one_subtasks_three_frames(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(1, "2-3;8"),
            task_flow_helper,
            [2, 3, 8],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_png(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(2, "5"),
            task_flow_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_exr(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(2, "5", "exr"),
            task_flow_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_two_frames(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(2, "5;9"),
            task_flow_helper,
            [5, 9],
        )

    @pytest.mark.asyncio
    async def test_four_subtasks_two_frames(self, task_flow_helper):
        await self._simulate(
            self._get_cube_params(4, "6-7"),
            task_flow_helper,
            [6, 7],
        )

    @pytest.mark.asyncio
    async def test_discard(self, task_flow_helper):
        task_params = self._get_cube_params(4, "6-7")
        expected_frames = [6, 7]
        task_flow_helper.init_provider(self._get_app_callbacks)
        async with task_flow_helper.init_requestor(
                self._get_app_callbacks) as requestor_client:
            task_id = 'test_discard_task_id123'
            await task_flow_helper.create_cube_task(task_id, task_params)
            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            task_flow_helper.check_results(
                task_params["format"],
                expected_frames,
            )

            # Discard all
            discarded_subtask_ids = \
                await requestor_client.discard_subtasks(task_id, subtask_ids)
            assert discarded_subtask_ids == subtask_ids
            assert await requestor_client.has_pending_subtasks(task_id)
            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            task_flow_helper.check_results(
                task_params["format"],
                expected_frames,
            )

            # Discard single
            discarded_subtask_ids = await requestor_client.discard_subtasks(
                task_id,
                subtask_ids[:1],
            )
            assert discarded_subtask_ids == subtask_ids[:1]
            assert await requestor_client.has_pending_subtasks(task_id)
            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            assert len(subtask_ids) == 1
            task_flow_helper.check_results(
                task_params["format"],
                expected_frames,
            )
