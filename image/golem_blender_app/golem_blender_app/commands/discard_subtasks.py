from typing import List

from golem_task_api import dirutils
from golem_task_api.apputils.task import SubtaskStatus
from golem_task_api.apputils.task.database import DBTaskManager


def discard_subtasks(
        work_dir: dirutils.RequestorTaskDir,
        subtask_ids: List[str],
) -> List[str]:
    task_manager = DBTaskManager(work_dir)
    for subtask_id in subtask_ids:
        task_manager.update_subtask_status(
            subtask_id,
            SubtaskStatus.ABORTED)
    return subtask_ids
