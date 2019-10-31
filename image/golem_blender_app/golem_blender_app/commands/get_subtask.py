from typing import List
import json

from golem_task_api import dirutils, structs

from golem_blender_app.commands import utils


def get_next_subtask(
        work_dir: dirutils.RequestorTaskDir,
        subtask_id: str,
) -> structs.Subtask:
    with open(work_dir / 'task_params.json', 'r') as f:
        task_params = json.load(f)
    with utils.get_db_connection(work_dir) as db:
        part_num = utils.get_next_pending_subtask(db)
        if part_num is None:
            raise Exception('No available subtasks at the moment')
        print(f'Part number: {part_num}, id: {subtask_id}')
        utils.start_subtask(
            db,
            part_num,
            subtask_id)

    scene_file = utils.get_scene_file_from_resources(task_params['resources'])
    all_frames = utils.string_to_frames(task_params['frames'])

    frames, parts = _choose_frames(
        all_frames,
        part_num,
        task_params['subtasks_count'],
    )
    min_y = (part_num % parts) / parts
    max_y = (part_num % parts + 1) / parts

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
        params=subtask_params,
        resources=resources,
    )


def _choose_frames(
        frames: List[str],
        part_num: int,
        total_subtasks: int) -> List[str]:
    if total_subtasks > len(frames):
        parts = total_subtasks // len(frames)
        return [frames[part_num // parts]], parts
    frames_per_subtask = (len(frames) + total_subtasks - 1) // total_subtasks
    start_frame = part_num * frames_per_subtask
    end_frame = min(start_frame + frames_per_subtask, len(frames))
    return frames[start_frame:end_frame], 1
