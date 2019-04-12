import json
import zipfile
import glob

from pathlib import Path

from . import utils
from .constants import TASK_PARAMS_FILE


def restart_task(
        old_work_dir: Path,
        old_network_resources_dir: Path,
        old_results_dir : Path,
        new_work_dir: Path,
        new_network_resources_dir: Path,
        new_results_dir : Path,) -> None:

    # _create_task

    with open(old_work_dir / TASK_PARAMS_FILE, 'r') as f:
        params = json.load(f)
    subtasks_count = params['subtasks_count']
    ## Copy task_params
    shutil.copy2(old_work_dir / TASK_PARAMS_FILE,
                 new_work_dir / TASK_PARAMS_FILE)
    ## Copy network_resources
    shutil.copy2(old_network_resources_dir,
                 new_network_resources_dir)
    ## Read fininshed subtasks from the old DB
    with utils.get_db_connection(old_work_dir) as db:
        finished_subtasks = util.get_subtasks_with_status(utils.SubtaskStatus.FINISHED)

    with utils.get_db_connection(new_work_dir) as db:
        utils.init_tables(db, subtasks_count)
        # Copy completed subtask results and update new DB
        for subtask_num in finished_subtasks:
            utils.set_subtask_status(db, subtask_num, utils.SubtaskStatus.FINISHED)
            for file in glob.glob(old_results_dir / 'subtask{subtask_num}-*'):
                shutil.copy2(file, new_results_dir)


    pass
