from golem_task_api import dirutils
from golem_task_api.apputils.task.database import DBTaskManager


def has_pending_subtasks(work_dir: dirutils.RequestorTaskDir) -> bool:
    return DBTaskManager(work_dir).get_next_computable_part_num() is not None
