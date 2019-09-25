from golem_task_api import dirutils

from golem_blender_app.commands import utils


def has_pending_subtasks(work_dir: dirutils.RequestorTaskDir) -> bool:
    with utils.get_db_connection(work_dir) as db:
        return utils.get_next_pending_subtask(db) is not None
