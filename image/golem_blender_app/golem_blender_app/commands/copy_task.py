import glob
import json
import shutil
import zipfile

from pathlib import Path

from . import utils
from .constants import TASK_PARAMS_FILE
from .verify import _collect_results


def copy_task(
        new_work_dir: Path,
        new_results_dir : Path,) -> None:

    # _create_task
    old_work_dir = new_work_dir / 'restarted'
    old_results_dir = new_results_dir / 'restarted'

    with open(old_work_dir / TASK_PARAMS_FILE, 'r') as f:
        params = json.load(f)
    subtasks_count = params['subtasks_count']
    ## Read fininshed subtasks from the old DB
    with utils.get_db_connection(old_work_dir) as db:
        finished_subtasks = utils.get_subtasks_with_status(db, utils.SubtaskStatus.FINISHED)

    with utils.get_db_connection(new_work_dir) as db:
        utils.init_tables(db, subtasks_count)
        # Copy completed subtask results and update new DB
        with open(old_work_dir / 'task_params.json', 'r') as f:
            task_params = json.load(f)
        for subtask_num in finished_subtasks:
            utils.update_subtask(db, subtask_num, utils.SubtaskStatus.FINISHED)
            subtask_glob = str(old_work_dir) + f'/subtask{subtask_num}-*/'
            for file in glob.glob(subtask_glob):
                old_subtask_dir  = Path(file)
                old_subtask_result_dir = old_subtask_dir / 'results'
                new_subtask_name = old_subtask_dir.name
                new_subtask_dir = new_work_dir / new_subtask_name
                new_subtask_dir.mkdir()
                new_subtask_result_dir = new_subtask_dir / 'results'
                shutil.copytree(old_subtask_result_dir, new_subtask_result_dir)
                with open(old_work_dir / f'{new_subtask_name}.json', 'r') as f:
                    params = json.load(f)
                _collect_results(
                    db,
                    subtask_num,
                    task_params,
                    params,
                    new_work_dir,
                    new_subtask_result_dir,
                    new_results_dir,
                )

    pass
