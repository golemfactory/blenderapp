from golem_blender_app.commands import utils
from golem_task_api import dirutils


def abort_task(work_dir: dirutils.RequestorTaskDir) -> None:
    with utils.get_db_connection(work_dir) as db:
        utils.abort_task(db)
