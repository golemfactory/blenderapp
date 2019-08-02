import asyncio
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
    TaskApiService,
    constants,
    ProviderAppClient,
    RequestorAppClient,
)


def wait_until_socket_open(host: str, port: int, timeout: float = 3.0) -> None:
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
        self.work_dir = work_dir
        self.req_work_dir = work_dir / 'requestor'
        self.req_work_dir.mkdir()
        self.prov_work_dir = work_dir / 'provider'
        self.prov_work_dir.mkdir()

    def init_provider(
            self,
            get_task_api_service: Callable[[Path], TaskApiService],
            task_id
    ) -> None:
        self.mkdir_provider_task(task_id)
        self._task_api_service = get_task_api_service(self.prov_task_work_dir)

    @asynccontextmanager
    async def start_provider(self) -> None:
        self.provider_client = ProviderAppClient(self._task_api_service)
        try:
            yield self.provider_client
        finally:
            await self.provider_client.shutdown()

    @asynccontextmanager
    async def init_requestor(
            self,
            get_task_api_service: Callable[[Path], TaskApiService],
            port: int = 50005,
    ):
        task_api_service = get_task_api_service(self.req_work_dir)
        self.requestor_client = RequestorAppClient(task_api_service, port)
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

    def put_resources_to_requestor(self, resources: List[Path]) -> None:
        for resource in resources:
            shutil.copy2(resource, self.req_task_resources_dir)

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

    async def create_task(
            self,
            task_id: str,
            max_subtasks_count: int,
            resources: List[Path],
            task_params: dict,
    ) -> None:
        self.mkdir_requestor(task_id)

        self.put_resources_to_requestor(resources)

        await self.requestor_client.create_task(
            task_id,
            max_subtasks_count,
            task_params,
        )

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

        async with self.start_provider():
            output_filepath = await self.provider_client.compute(
                task_id,
                subtask.subtask_id,
                subtask.params,
            )
        self.copy_result_from_provider(output_filepath, subtask.subtask_id)

        verdict = \
            await self.requestor_client.verify(task_id, subtask.subtask_id)
        return (subtask.subtask_id, verdict)

    async def run_provider_benchmark(self) -> float:
        result = 0.0
        async with self.start_provider():
            result = await self.provider_client.run_benchmark()
        return result

    async def shutdown_provider(self) -> None:
        return await self.provider_client.shutdown()


@pytest.fixture
def task_flow_helper(tmpdir):
    return TaskFlowHelper(Path(tmpdir))


class SimulationBase(abc.ABC):
    DEFAULT_RESOLUTION = [1000, 600]
    @abc.abstractmethod
    def _get_task_api_service(
            self,
            work_dir: Path,
    ) -> TaskApiService:
        pass

    @staticmethod
    def _get_cube_params(
            frames: str,
            output_format: str = "png",
            resolution: List[int] = DEFAULT_RESOLUTION,
    ):
        return {
            "format": output_format,
            "resolution": resolution,
            "frames": frames,
            "scene_file": "cube.blend",
            "resources": [
                "cube.blend",
            ]
        }

    @staticmethod
    def _get_cube_resources() -> List[Path]:
        return [Path(__file__).parent / 'resources' / 'cube.blend']

    @staticmethod
    def check_results(
            req_task_results_dir: Path,
            output_format: str,
            expected_frames: List[int],
    ) -> None:
        for frame in expected_frames:
            filename = f'result{frame:04d}.{output_format}'
            result_file = req_task_results_dir / filename
            assert result_file.exists()

    async def _simulate(
            self,
            max_subtasks_count: int,
            task_params: dict,
            task_flow_helper: TaskFlowHelper,
            expected_frames: list,
    ):
        async with task_flow_helper.init_requestor(
                self._get_task_api_service) as requestor_client:
            task_id = 'test_task_id123'
            await task_flow_helper.create_task(
                task_id,
                max_subtasks_count,
                self._get_cube_resources(),
                task_params,
            )
            task_flow_helper.init_provider(self._get_task_api_service, task_id)
            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            assert len(subtask_ids) <= max_subtasks_count

            assert not await requestor_client.has_pending_subtasks(task_id)
            self.check_results(
                task_flow_helper.req_task_results_dir,
                task_params["format"],
                expected_frames,
            )

    @pytest.mark.asyncio
    async def test_one_subtasks_one_frame(self, task_flow_helper):
        await self._simulate(
            1,
            self._get_cube_params("1"),
            task_flow_helper,
            [1],
        )

    @pytest.mark.asyncio
    async def test_one_subtasks_three_frames(self, task_flow_helper):
        await self._simulate(
            1,
            self._get_cube_params("2-3;8"),
            task_flow_helper,
            [2, 3, 8],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_png(self, task_flow_helper):
        await self._simulate(
            2,
            self._get_cube_params("5"),
            task_flow_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_exr(self, task_flow_helper):
        await self._simulate(
            2,
            self._get_cube_params("5", "exr"),
            task_flow_helper,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_two_frames(self, task_flow_helper):
        await self._simulate(
            2,
            self._get_cube_params("5;9"),
            task_flow_helper,
            [5, 9],
        )

    @pytest.mark.asyncio
    async def test_four_subtasks_two_frames(self, task_flow_helper):
        await self._simulate(
            4,
            self._get_cube_params("6-7"),
            task_flow_helper,
            [6, 7],
        )

    @pytest.mark.asyncio
    async def test_discard(self, task_flow_helper):
        max_subtasks_count = 4
        task_params = self._get_cube_params("6-7")
        expected_frames = [6, 7]
        async with task_flow_helper.init_requestor(
                self._get_task_api_service) as requestor_client:
            task_id = 'test_discard_task_id123'
            await task_flow_helper.create_task(
                task_id,
                max_subtasks_count,
                self._get_cube_resources(),
                task_params,
            )
            task_flow_helper.init_provider(self._get_task_api_service, task_id)
            subtask_ids = \
                await task_flow_helper.compute_remaining_subtasks(task_id)
            self.check_results(
                task_flow_helper.req_task_results_dir,
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
            self.check_results(
                task_flow_helper.req_task_results_dir,
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
            self.check_results(
                task_flow_helper.req_task_results_dir,
                task_params["format"],
                expected_frames,
            )


    @pytest.mark.asyncio
    async def test_provider_single_shutdown(self, task_flow_helper):
        print("init_provider")
        task_flow_helper.init_provider(self._get_task_api_service, 'task123')
        async with task_flow_helper.start_provider():
            print("await shutdown")
        print("done!")

    @pytest.mark.asyncio
    async def test_provider_double_shutdown(self, task_flow_helper):
        print("init_provider")
        task_flow_helper.init_provider(self._get_task_api_service, 'task123')
        async with task_flow_helper.start_provider():
            print("shutdown 1")
            await task_flow_helper.shutdown_provider()
            print("shutdown 2")
        print("done!")

    @pytest.mark.asyncio
    async def test_provider_shutdown_in_benchmark(self, task_flow_helper):
        benchmark_defer = asyncio.ensure_future(self._simulate(
            1,
            self._get_cube_params("1", resolution=[10000, 6000]),
            task_flow_helper,
            [1],
        ))
        async def _shutdown_in_5s():
            await asyncio.sleep(5.0)
            await task_flow_helper.shutdown_provider()
            return None
        shutdown_defer = asyncio.ensure_future(_shutdown_in_5s())
        done, pending = await asyncio.wait(
            [shutdown_defer, benchmark_defer],
            return_when=asyncio.FIRST_COMPLETED)
        assert benchmark_defer in pending
        assert shutdown_defer in done
        try:
            await benchmark_defer
        except Exception as e:
            print("Benchmark should fail when shutting down. Error=", e)
        print('DONE')
