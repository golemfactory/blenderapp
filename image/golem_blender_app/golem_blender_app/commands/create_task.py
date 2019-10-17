import json
import zipfile


from golem_blender_app import constants
from golem_blender_app.commands import utils
from golem_task_api import dirutils, envs, structs


def create_task(
        work_dir: dirutils.RequestorTaskDir,
        max_subtasks_count: int,
        params: dict
) -> structs.Task:
    frame_count = len(utils.string_to_frames(params['frames']))
    if max_subtasks_count <= frame_count:
        subtasks_count = max_subtasks_count
    else:
        subtasks_count = max_subtasks_count // frame_count * frame_count
    params['subtasks_count'] = subtasks_count

    if not utils.get_scene_file_from_resources(params['resources']):
        raise RuntimeError("Scene file not found in resources")

    with zipfile.ZipFile(work_dir.subtask_inputs_dir / '0.zip', 'w') as zipf:
        for resource in params['resources']:
            resource_path = work_dir.task_inputs_dir / resource
            zipf.write(resource_path, resource)

    with open(work_dir / 'task_params.json', 'w') as f:
        json.dump(params, f)

    with utils.get_db_connection(work_dir) as db:
        utils.init_tables(db, subtasks_count)

    return envs.create_docker_cpu_task(
        image=constants.DOCKER_IMAGE,
        tag=constants.VERSION
    )
