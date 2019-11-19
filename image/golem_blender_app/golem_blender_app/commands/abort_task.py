from golem_task_api.apputils.task.database import DBTaskManager
from golem_task_api import dirutils


def abort_task(work_dir: dirutils.RequestorTaskDir) -> None:
    DBTaskManager(work_dir).abort_task()
