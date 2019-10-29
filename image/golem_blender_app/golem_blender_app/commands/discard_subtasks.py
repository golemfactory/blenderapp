from typing import List

from golem_task_api import dirutils
from golem_blender_app.commands import utils


def discard_subtasks(
        work_dir: dirutils.RequestorTaskDir,
        subtask_ids: List[str],
) -> List[str]:
    with utils.get_db_connection(work_dir) as db:
        for subtask_id in subtask_ids:
            utils.update_subtask_status(
                db,
                subtask_id,
                utils.SubtaskStatus.ABORTED)
    return subtask_ids
