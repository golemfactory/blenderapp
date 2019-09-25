from typing import List

from golem_task_api import dirutils
from golem_blender_app.commands import utils


def discard_subtasks(
        work_dir: dirutils.RequestorTaskDir,
        subtask_ids: List[str],
) -> List[str]:
    with utils.get_db_connection(work_dir) as db:
        for subtask_id in subtask_ids:
            subtask_num = utils.get_subtask_num_from_id(subtask_id)
            utils.update_subtask(db, subtask_num, utils.SubtaskStatus.PENDING)
    return subtask_ids
