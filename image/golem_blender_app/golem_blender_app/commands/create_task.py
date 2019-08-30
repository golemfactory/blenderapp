import json
import zipfile

from pathlib import Path

from golem_blender_app.commands import utils
from golem_task_api import constants


def create_task(work_dir: Path, max_subtasks_count: int, params: dict) -> None:
    task_inputs_dir = work_dir / constants.TASK_INPUTS_DIR
    subtask_inputs_dir = work_dir / constants.SUBTASK_INPUTS_DIR

    frame_count = len(utils.string_to_frames(params['frames']))
    if max_subtasks_count <= frame_count:
        subtasks_count = max_subtasks_count
    else:
        subtasks_count = max_subtasks_count // frame_count * frame_count
    params['subtasks_count'] = subtasks_count
    with open(work_dir / 'task_params.json', 'w') as f:
        json.dump(params, f)

    with zipfile.ZipFile(subtask_inputs_dir / '0.zip', 'w') as zipf:
        for resource in params['resources']:
            resource_path = task_inputs_dir / resource
            zipf.write(resource_path, resource)

    with utils.get_db_connection(work_dir) as db:
        utils.init_tables(db, subtasks_count)
