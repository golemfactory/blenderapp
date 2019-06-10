from pathlib import Path
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


class SetupHelper:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.req_work_dir = work_dir / 'requestor'
        self.req_work_dir.mkdir()
        self.prov_work_dir = work_dir / 'provider'
        self.prov_work_dir.mkdir()

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

    def mkdir_provider_subtask(self, subtask_id: str):
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


@pytest.fixture
def setup_helper(tmpdir):
    return SetupHelper(Path(tmpdir))


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
            setup_helper: SetupHelper,
            expected_frames: list,
    ):
        print('workdir:', setup_helper.work_dir)

        requestor_port = 50005
        requestor_callbacks = self._get_requestor_app_callbacks(
            setup_helper.req_work_dir)

        requestor_client = RequestorAppClient(
            requestor_callbacks,
            requestor_port,
        )
        try:
            # Wait for the servers to be ready, I couldn't find a reliable
            # way for that check
            await asyncio.sleep(1)

            task_id = 'test_task_id123'
            setup_helper.mkdir_requestor(task_id)
            setup_helper.mkdir_provider_task(task_id)

            setup_helper.put_cube_to_resources()

            await requestor_client.create_task(task_id, task_params)

            for _ in range(task_params['subtasks_count']):

                subtask_id, subtask_params = \
                    await requestor_client.next_subtask(task_id)
                assert subtask_params['resources'] == [0]

                setup_helper.copy_resources_from_requestor(subtask_params)
                setup_helper.mkdir_provider_subtask(subtask_id)

                await ProviderAppClient.compute(
                    self._get_provider_app_callbacks(
                        setup_helper.prov_task_work_dir),
                    setup_helper.prov_task_work_dir,
                    task_id,
                    subtask_id,
                    subtask_params,
                )
                setup_helper.copy_result_from_provider(subtask_id)

                success = await requestor_client.verify(task_id, subtask_id)
                assert success

            for frame in expected_frames:
                filename = f'result{frame:04d}.{task_params["format"]}'
                result_file = setup_helper.req_task_results_dir / filename
                assert result_file.exists()
        finally:
            await requestor_client.shutdown()
            await requestor_callbacks.wait_after_shutdown()

    @pytest.mark.asyncio
    async def test_one_subtasks_one_frame(self, setup_helper):
        await self._simulate(
            self._get_cube_params(1, "1"),
            setup_helper,
            [1],
        )

    @pytest.mark.asyncio
    async def test_one_subtasks_three_frames(self, setup_helper):
        await self._simulate(
            self._get_cube_params(1, "2-3;8"),
            setup_helper,
            [2, 3, 8],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_png(self, setup_helper):
        await self._simulate(
            self._get_cube_params(2, "5"),
            setup_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_exr(self, setup_helper):
        await self._simulate(
            self._get_cube_params(2, "5", "exr"),
            setup_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_two_frames(self, setup_helper):
        await self._simulate(
            self._get_cube_params(2, "5;9"),
            setup_helper,
            [5, 9],
        )

    @pytest.mark.asyncio
    async def test_four_subtasks_two_frames(self, setup_helper):
        await self._simulate(
            self._get_cube_params(4, "6-7"),
            setup_helper,
            [6, 7],
        )
