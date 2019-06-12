from async_generator import asynccontextmanager
from pathlib import Path
from typing import Callable, List
import abc
import asyncio
import random
import shutil
import pytest

from golem_task_api import (
    constants,
    ProviderAppCallbacks,
    ProviderAppClient,
    RequestorAppCallbacks,
    RequestorAppClient,
)


class TaskFlowHelper:
    def __init__(self, work_dir: Path) -> None:
        print('workdir:', work_dir)
        self.work_dir = work_dir
        self.req_work_dir = work_dir / 'requestor'
        self.req_work_dir.mkdir()
        self.prov_work_dir = work_dir / 'provider'
        self.prov_work_dir.mkdir()

    def init_provider(
            self,
            get_app_callbacks: Callable[[Path], ProviderAppCallbacks],
    ) -> None:
        self.get_provider_app_callbacks = get_app_callbacks

    @asynccontextmanager
    async def init_requestor(
            self,
            get_app_callbacks: Callable[[Path], RequestorAppCallbacks],
            port: int = 50005,
    ):
        app_callbacks = get_app_callbacks(self.req_work_dir)
        self.requestor_client = RequestorAppClient(app_callbacks, port)
        try:
            # Wait for the servers to be ready, I couldn't find a reliable
            # way for that check
            await asyncio.sleep(1)
            yield
        finally:
            await self.requestor_client.shutdown()
            await app_callbacks.wait_after_shutdown()

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

    def copy_resources_from_requestor(self, subtask_params: dict) -> None:
        for resource_id in subtask_params['resources']:
            network_resource = \
                self.req_task_net_resources_dir / f'{resource_id}.zip'
            assert network_resource.exists()
            shutil.copy2(network_resource, self.prov_task_net_resources_dir)

    def copy_result_from_provider(self, subtask_id: str) -> None:
        result = self.prov_task_work_dir / subtask_id / 'result.zip'
        assert result.exists()
        shutil.copy2(
            result,
            self.req_task_net_results_dir / f'{subtask_id}.zip',
        )

    async def create_cube_task(self, task_id: str, task_params: dict) -> None:
        self.mkdir_requestor(task_id)
        self.mkdir_provider_task(task_id)

        self.put_cube_to_resources()

        await self.requestor_client.create_task(task_id, task_params)

    async def compute_next_subtask(self, task_id: str) -> bool:
        subtask_id, subtask_params = \
            await self.requestor_client.next_subtask(task_id)
        assert subtask_params['resources'] == [0]

        self.copy_resources_from_requestor(subtask_params)
        self.mkdir_provider_subtask(subtask_id)

        await ProviderAppClient.compute(
            self.get_provider_app_callbacks(self.prov_task_work_dir),
            self.prov_task_work_dir,
            task_id,
            subtask_id,
            subtask_params,
        )
        self.copy_result_from_provider(subtask_id)

        return await self.requestor_client.verify(task_id, subtask_id)

    async def run_provider_benchmark(self) -> float:
        return await ProviderAppClient.run_benchmark(
            self.get_provider_app_callbacks(self.prov_work_dir),
            self.prov_work_dir,
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
    def _get_requestor_app_callbacks(
            self,
            work_dir: Path,
    ) -> RequestorAppCallbacks:
        pass

    @abc.abstractmethod
    def _get_provider_app_callbacks(
            self,
            work_dir: Path,
    ) -> ProviderAppCallbacks:
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
        task_flow_helper.init_provider(self._get_provider_app_callbacks)
        async with task_flow_helper.init_requestor(
                self._get_requestor_app_callbacks):
            task_id = 'test_task_id123'
            await task_flow_helper.create_cube_task(task_id, task_params)

            for _ in range(task_params['subtasks_count']):
                success = await task_flow_helper.compute_next_subtask(task_id)
                assert success

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
