from pathlib import Path

from typing import Tuple, Optional
import json
import shutil
import zipfile

from golem_task_api.apputils.task import SubtaskStatus
from golem_task_api.apputils.task.database import DBTaskManager
from golem_task_api import dirutils, enums

from golem_blender_app.commands import utils
from golem_blender_app.commands.renderingtaskcollector import (
    RenderingTaskCollector
)
from golem_blender_app.verifier_tools import verifier
from golem_blender_app.verifier_tools.file_extension.matcher import \
    get_expected_extension

async def verify(
        work_dir: dirutils.RequestorTaskDir,
        subtask_id: str,
) -> Tuple[enums.VerifyResult, Optional[str]]:
    with open(work_dir / 'task_params.json', 'r') as f:
        task_params = json.load(f)
    with open(work_dir / f'subtask{subtask_id}.json', 'r') as f:
        params = json.load(f)
    subtask_work_dir = work_dir / f'subtask{subtask_id}'
    subtask_work_dir.mkdir()
    subtask_results_dir = subtask_work_dir / 'results'
    subtask_results_dir.mkdir()
    subtask_output_dir = subtask_work_dir / 'output'
    subtask_output_dir.mkdir()

    subtask_outputs_dir = work_dir.subtask_outputs_dir(subtask_id)
    zip_file_path = subtask_outputs_dir / f'{subtask_id}.zip'
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        zip_file.extractall(subtask_results_dir)

    task_manager = DBTaskManager(work_dir)
    part_num = task_manager.get_part_num(subtask_id)
    task_manager.update_subtask_status(subtask_id, SubtaskStatus.VERIFYING)
    dir_contents = subtask_results_dir.iterdir()

    verdict = await verifier.verify(
        [str(entry) for entry in dir_contents if entry.is_file()],
        params['borders'],
        work_dir.task_inputs_dir / params['scene_file'],
        params['resolution'],
        params['samples'],
        params['frames'],
        params['output_format'],
        mounted_paths={
            'OUTPUT_DIR': str(subtask_output_dir),
            'WORK_DIR': str(subtask_work_dir),
        }
    )
    print("Verdict:", verdict)
    if not verdict:
        task_manager.update_subtask_status(
            subtask_id,
            SubtaskStatus.FAILURE)
        # pylint: disable=fixme
        # TODO: provide some extra info why verification failed
        return enums.VerifyResult.FAILURE, None

    task_manager.update_subtask_status(subtask_id, SubtaskStatus.SUCCESS)
    _collect_results(
        task_manager,
        part_num,
        task_params,
        params,
        work_dir,
        subtask_results_dir,
        work_dir.task_outputs_dir,
    )
    return enums.VerifyResult.SUCCESS, None


def _collect_results(
        task_manager: DBTaskManager,
        part_num: int,
        task_params: dict,
        params: dict,
        work_dir: Path,
        subtask_results_dir: Path,
        results_dir: Path) -> None:
    frames = utils.string_to_frames(task_params['frames'])
    frame_count = len(frames)
    out_format = get_expected_extension(params['output_format'])
    parts = task_params['subtasks_count'] // frame_count
    if parts <= 1:
        for frame in params['frames']:
            shutil.copy2(
                subtask_results_dir / f'result{frame:04d}.{out_format}',
                results_dir / f'result{frame:04d}.{out_format}',
            )
        return

    frame_id = part_num // parts
    frame = frames[frame_id]
    subtasks_nums = list(range(frame_id * parts, (frame_id + 1) * parts))
    subtasks_statuses = task_manager.get_subtasks_statuses(subtasks_nums)
    all_finished = all([
        s[0] == SubtaskStatus.SUCCESS for s in subtasks_statuses.values()
    ])
    if not all_finished:
        print('Not all finished, waiting for more results')
        return

    print('All finished, collecting results')
    collector = RenderingTaskCollector(
        width=params['resolution'][0],
        height=params['resolution'][1],
    )
    for i in subtasks_nums[::-1]:
        result_dir = work_dir / f'subtask{subtasks_statuses[i][1]}' / 'results'
        result_img = result_dir / f'result{frame:04d}.{out_format}'
        print(f'result_dir: {result_dir}')
        for file in result_dir.iterdir():
            print(f'result_candidate: {file}')
        print(f"result_img:{result_img.exists()}")
        print(f"result_img.size:{result_img.stat()}")
        collector.add_img_file(str(result_img))

    image = collector.finalize()
    if not image:
        raise RuntimeError("No accepted image files")

    with image as img:
        img.save_with_extension(
            results_dir / f'result{frame:04d}',
            out_format)
