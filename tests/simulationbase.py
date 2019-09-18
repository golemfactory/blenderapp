from pathlib import Path
from typing import List
import abc
import asyncio
import contextlib
import pytest
import socket
import time

from golem_task_api import TaskApiService
from golem_task_api.client import ShutdownException
from golem_task_api.testutils import TaskLifecycleUtil


@pytest.fixture
def task_lifecycle_util(tmpdir):
    print('workdir:', tmpdir)
    return TaskLifecycleUtil(Path(tmpdir))


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
    def _check_results(
            req_task_outputs_dir: Path,
            output_format: str,
            expected_frames: List[int],
    ) -> None:
        for frame in expected_frames:
            filename = f'result{frame:04d}.{output_format}'
            result_file = req_task_outputs_dir / filename
            assert result_file.exists()

    async def _simulate_cube_task(
        self,
        max_subtasks_count: int,
        task_params: dict,
        task_lifecycle_util: TaskLifecycleUtil,
        expected_frames: List[int],
    ):
        await task_lifecycle_util.simulate_task(
            self._get_task_api_service,
            max_subtasks_count,
            task_params,
            self._get_cube_resources(),
        )
        self._check_results(
            task_lifecycle_util.req_task_outputs_dir,
            task_params["format"],
            expected_frames,
        )

    @pytest.mark.asyncio
    async def test_one_subtasks_one_frame(self, task_lifecycle_util):
        await self._simulate_cube_task(
            1,
            self._get_cube_params("1"),
            task_lifecycle_util,
            [1],
        )

    @pytest.mark.asyncio
    async def test_one_subtasks_three_frames(self, task_lifecycle_util):
        await self._simulate_cube_task(
            1,
            self._get_cube_params("2-3;8"),
            task_lifecycle_util,
            [2, 3, 8],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_png(self, task_lifecycle_util):
        await self._simulate_cube_task(
            2,
            self._get_cube_params("5"),
            task_lifecycle_util,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_one_frame_exr(self, task_lifecycle_util):
        await self._simulate_cube_task(
            2,
            self._get_cube_params("5", "exr"),
            task_lifecycle_util,
            [5],
        )

    @pytest.mark.asyncio
    async def test_two_subtasks_two_frames(self, task_lifecycle_util):
        await self._simulate_cube_task(
            2,
            self._get_cube_params("5;9"),
            task_lifecycle_util,
            [5, 9],
        )

    @pytest.mark.asyncio
    async def test_four_subtasks_two_frames(self, task_lifecycle_util):
        await self._simulate_cube_task(
            4,
            self._get_cube_params("6-7"),
            task_lifecycle_util,
            [6, 7],
        )

    @pytest.mark.asyncio
    async def test_discard(self, task_lifecycle_util):
        max_subtasks_count = 4
        task_params = self._get_cube_params("6-7")
        expected_frames = [6, 7]
        async with task_lifecycle_util.init_requestor(
                self._get_task_api_service) as requestor_client:
            task_id = 'test_discard_task_id123'
            await task_lifecycle_util.create_task(
                task_id,
                max_subtasks_count,
                self._get_cube_resources(),
                task_params,
            )
            task_lifecycle_util.init_provider(
                self._get_task_api_service,
                task_id,
            )
            subtask_ids = await task_lifecycle_util.compute_remaining_subtasks(
                task_id=task_id,
                opaque_node_id='whatever'
            )
            self._check_results(
                task_lifecycle_util.req_task_outputs_dir,
                task_params["format"],
                expected_frames,
            )

            # Discard all
            discarded_subtask_ids = \
                await requestor_client.discard_subtasks(task_id, subtask_ids)
            assert discarded_subtask_ids == subtask_ids
            assert await requestor_client.has_pending_subtasks(task_id)
            subtask_ids = await task_lifecycle_util.compute_remaining_subtasks(
                task_id=task_id,
                opaque_node_id='whatever'
            )
            self._check_results(
                task_lifecycle_util.req_task_outputs_dir,
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
            subtask_ids = await task_lifecycle_util.compute_remaining_subtasks(
                task_id=task_id,
                opaque_node_id='whatever'
            )
            assert len(subtask_ids) == 1
            self._check_results(
                task_lifecycle_util.req_task_outputs_dir,
                task_params["format"],
                expected_frames,
            )

    @pytest.mark.asyncio
    async def test_provider_single_shutdown(self, task_lifecycle_util):
        print("init_provider")
        task_lifecycle_util.init_provider(self._get_task_api_service, 'task123')
        print("start_provider")
        await task_lifecycle_util.start_provider()
        print("shutdown 1")
        await task_lifecycle_util.shutdown_provider()
        print("done!")

    @pytest.mark.asyncio
    async def test_provider_double_shutdown(self, task_lifecycle_util):
        print("init_provider")
        task_lifecycle_util.init_provider(self._get_task_api_service, 'task123')
        print("start_provider")
        await task_lifecycle_util.start_provider()
        print("shutdown 1")
        await task_lifecycle_util.shutdown_provider()
        print("shutdown 2")
        await task_lifecycle_util.shutdown_provider()
        print("done!")

    @pytest.mark.asyncio
    async def test_provider_shutdown_in_benchmark(self, task_lifecycle_util):
        benchmark_defer = asyncio.ensure_future(self._simulate_cube_task(
            1,
            self._get_cube_params("1", resolution=[10000, 6000]),
            task_lifecycle_util,
            [1],
        ))

        async def _shutdown_in_5s():
            await asyncio.sleep(5.0)
            await task_lifecycle_util.shutdown_provider()
            return None
        shutdown_defer = asyncio.ensure_future(_shutdown_in_5s())

        done, _ = await asyncio.wait(
            [shutdown_defer, benchmark_defer],
            return_when=asyncio.FIRST_COMPLETED)
        assert shutdown_defer in done

        with pytest.raises(ShutdownException):
            await benchmark_defer
