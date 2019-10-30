from pathlib import Path
from typing import Tuple, Optional
import json
import os
import shutil
import zipfile

from golem_blender_app.commands import utils
from golem_blender_app.commands.renderingtaskcollector import (
    RenderingTaskCollector
)
from golem_blender_app.verifier_tools import verificator
from golem_task_api import dirutils, enums


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
    with zipfile.ZipFile(subtask_outputs_dir / f'{subtask_id}.zip', 'r') as f:
        f.extractall(subtask_results_dir)

    with utils.get_db_connection(work_dir) as db:
        subtask_num = utils.get_subtask_num(db, subtask_id)
        utils.update_subtask_status(
            db,
            subtask_id,
            utils.SubtaskStatus.VERIFYING)
        dir_contents = subtask_results_dir.iterdir()

        verdict = await verificator.verify(
            [str(f) for f in dir_contents if f.is_file()],
            params['borders'],
            work_dir.task_inputs_dir / params['scene_file'],
            params['resolution'],
            params['samples'],
            params['frames'],
            params['output_format'],
            'verify',
            mounted_paths={
                'OUTPUT_DIR': str(subtask_output_dir),
                'WORK_DIR': str(subtask_work_dir),
            }
        )
        print("Verdict:", verdict)
        if not verdict:
            utils.update_subtask_status(
                db,
                subtask_id,
                utils.SubtaskStatus.FAILED)
            # TODO: provide some extra info why verification failed
            return enums.VerifyResult.FAILURE, None
        utils.update_subtask_status(
            db,
            subtask_id,
            utils.SubtaskStatus.FINISHED)
        _collect_results(
            db,
            subtask_num,
            task_params,
            params,
            work_dir,
            subtask_results_dir,
            work_dir.task_outputs_dir,
        )
        return enums.VerifyResult.SUCCESS, None


def _collect_results(
        db,
        subtask_num: int,
        task_params: dict,
        params: dict,
        work_dir: Path,
        subtask_results_dir: Path,
        results_dir: Path) -> None:
    frames = utils.string_to_frames(task_params['frames'])
    frame_count = len(frames)
    out_format = params['output_format']
    parts = task_params['subtasks_count'] // frame_count
    if parts <= 1:
        for frame in params['frames']:
            shutil.copy2(
                subtask_results_dir / f'result{frame:04d}.{out_format}',
                results_dir / f'result{frame:04d}.{out_format}',
            )
        return

    frame_id = subtask_num // parts
    frame = frames[frame_id]
    subtasks_nums = list(range(frame_id * parts, (frame_id + 1) * parts))
    subtasks_statuses = utils.get_subtasks_statuses(db, subtasks_nums)
    all_finished = all([
        s[0] == utils.SubtaskStatus.FINISHED for s in subtasks_statuses.values()
    ])
    if not all_finished:
        return

    collector = RenderingTaskCollector(
        width=params['resolution'][0],
        height=params['resolution'][1],
    )
    for i in subtasks_nums[::-1]:
        collector.add_img_file(
            str(work_dir / f'subtask{subtasks_statuses[i][1]}' / 'results' / f'result{frame:04d}.{out_format}'),  # noqa
        )
    with collector.finalize() as image:
        image.save_with_extension(
            results_dir / f'result{frame:04d}',
            out_format,
        )
