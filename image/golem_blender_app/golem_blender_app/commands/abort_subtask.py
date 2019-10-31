from golem_blender_app.commands import utils
from golem_task_api import dirutils


def abort_subtask(
        work_dir: dirutils.RequestorTaskDir,
        subtask_id: str
) -> None:
    with utils.get_db_connection(work_dir) as db:
        utils.update_subtask_status(db, subtask_id, utils.SubtaskStatus.ABORTED)
