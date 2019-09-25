import os
import zipfile
from pathlib import Path

from golem_task_api.dirutils import ProviderTaskDir

from golem_blender_app.render_tools import blender_render


async def compute(
    work_dir: ProviderTaskDir,
    subtask_id: str,
    subtask_params: dict
) -> Path:
    params = subtask_params
    subtask_work_dir = work_dir.subtask_dir(subtask_id)
    resources_dir = work_dir / 'extracted_subtask_inputs'
    result_dir = subtask_work_dir / 'result'
    result_dir.mkdir()
    for rid in params['resources']:
        with zipfile.ZipFile(work_dir.subtask_inputs_dir / rid, 'r') as zipf:
            zipf.extractall(resources_dir)

    params['scene_file'] = resources_dir / params['scene_file']
    params['crops'] = [{
        'outfilebasename': 'result',
        'borders_x': [params['borders'][0], params['borders'][2]],
        'borders_y': [params['borders'][1], params['borders'][3]],
    }]
    params.pop('borders')
    await blender_render.render(
        params,
        {
            "WORK_DIR": str(subtask_work_dir),
            "OUTPUT_DIR": str(result_dir),
        },
    )

    output_filepath = f'{subtask_id}.zip'
    with zipfile.ZipFile(work_dir / output_filepath, 'w') as zipf:
        for filename in os.listdir(result_dir):
            zipf.write(result_dir / filename, filename)
            # FIXME delete raw files ?

    return output_filepath
