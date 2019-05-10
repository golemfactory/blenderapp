import json
import zipfile

from pathlib import Path

from golem_blender_app.commands import utils
from golem_task_api import constants


def create_task(work_dir: Path, params: dict) -> None:
    resources_dir = work_dir / constants.RESOURCES_DIR
    network_resources_dir = work_dir / constants.NETWORK_RESOURCES_DIR
    with open(work_dir / 'task_params.json', 'w') as f:
        json.dump(params, f)
    frame_count = len(utils.string_to_frames(params['frames']))
    subtasks_count = params['subtasks_count']
    assert subtasks_count <= frame_count or subtasks_count % frame_count == 0
    with zipfile.ZipFile(network_resources_dir / '0.zip', 'w') as zipf:
        for resource in params['resources']:
            resource_path = resources_dir / resource
            zipf.write(resource_path, resource)

    with utils.get_db_connection(work_dir) as db:
        utils.init_tables(db, subtasks_count)
