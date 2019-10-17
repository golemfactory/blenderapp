from typing import List
import json

from golem_task_api import dirutils, structs

from golem_blender_app.commands import utils


def get_next_subtask(
        work_dir: dirutils.RequestorTaskDir
) -> structs.Subtask:
    with open(work_dir / 'task_params.json', 'r') as f:
        task_params = json.load(f)
    with utils.get_db_connection(work_dir) as db:
        subtask_num = utils.get_next_pending_subtask(db)
        if subtask_num is None:
            raise Exception('No available subtasks at the moment')
        subtask_id = utils.gen_subtask_id(subtask_num)
        print(f'Subtask number: {subtask_num}, id: {subtask_id}')
        utils.update_subtask(
            db,
            subtask_num,
            utils.SubtaskStatus.COMPUTING,
            subtask_id,
        )

    scene_file = utils.get_scene_file_from_resources(task_params['resources'])
    all_frames = utils.string_to_frames(task_params['frames'])

    frames, parts = _choose_frames(
        all_frames,
        subtask_num,
        task_params['subtasks_count'],
    )
    min_y = (subtask_num % parts) / parts
    max_y = (subtask_num % parts + 1) / parts

    resources = ['0.zip']
    subtask_params = {
        "scene_file": scene_file,
        "resolution": task_params['resolution'],
        "use_compositing": False,
        "samples": 0,
        "frames": frames,
        "output_format": task_params['format'],
        "borders": [0.0, min_y, 1.0, max_y],

        "resources": resources,
    }

    with open(work_dir / f'subtask{subtask_id}.json', 'w') as f:
        json.dump(subtask_params, f)

    return structs.Subtask(
        subtask_id=subtask_id,
        params=subtask_params,
        resources=resources,
    )


def _choose_frames(
        frames: List[str],
        subtask_num: int,
        total_subtasks: int) -> List[str]:
    if total_subtasks > len(frames):
        parts = total_subtasks // len(frames)
        return [frames[subtask_num // parts]], parts
    frames_per_subtask = (len(frames) + total_subtasks - 1) // total_subtasks
    start_frame = subtask_num * frames_per_subtask
    end_frame = min(start_frame + frames_per_subtask, len(frames))
    return frames[start_frame:end_frame], 1
