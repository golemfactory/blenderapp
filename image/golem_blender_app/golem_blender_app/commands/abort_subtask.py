from golem_task_api.apputils.task import SubtaskStatus
from golem_task_api.apputils.task.database import DBTaskManager
from golem_task_api import dirutils


def abort_subtask(
        work_dir: dirutils.RequestorTaskDir,
        subtask_id: str
) -> None:
    DBTaskManager(work_dir).update_subtask_status(
        subtask_id,
        SubtaskStatus.ABORTED)
