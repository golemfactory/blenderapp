import json
import zipfile
from copy import deepcopy

from golem_blender_app import constants
from golem_blender_app.commands import utils
from golem_blender_app.render_tools import blender_render
from golem_task_api import dirutils, envs, structs
from golem_task_api.apputils.task.database import DBTaskManager
from golem_task_api.structs import Infrastructure


async def test_task(
    work_dir: dirutils.RequestorTaskDir,
    params: dict,
) -> int:

    result_dir = work_dir / 'task_test'
    result_dir.mkdir(exist_ok=True)

    scene_file = work_dir.task_inputs_dir / utils.get_scene_file_from_resources(
        params['resources'])

    params = deepcopy(params)
    params.update({
        'scene_file': scene_file,
        'frames': [1],
        'output_format': 'png',
        'resolution': [200, 100],
        'crops': [{
            'outfilebasename': 'task_test',
            'borders_x': [0.0, 1.0],
            'borders_y': [0.0, 1.0],
        }],
        'use_compositing': False,
        'samples': 0,
    })

    results = await blender_render.render(
        params,
        {
            "WORK_DIR": str(work_dir.task_inputs_dir),
            "OUTPUT_DIR": str(result_dir),
        },
        monitor_usage=True,
    )

    result = max(results, key=lambda r: r["usage"].mem_peak)
    return result["usage"].mem_peak


async def create_task(
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

    min_memory = await test_task(work_dir, params)

    with zipfile.ZipFile(work_dir.subtask_inputs_dir / '0.zip', 'w') as zipf:
        for resource in params['resources']:
            resource_path = work_dir.task_inputs_dir / resource
            zipf.write(resource_path, resource)

    with open(work_dir / 'task_params.json', 'w') as f:
        json.dump(params, f)

    DBTaskManager(work_dir).create_task(subtasks_count)

    return envs.create_docker_cpu_task(
        image=constants.DOCKER_IMAGE,
        tag=constants.VERSION,
        inf=Infrastructure(min_memory_mib=min_memory // (1024 * 1024))
    )
