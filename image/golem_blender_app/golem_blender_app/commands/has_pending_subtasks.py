from pathlib import Path

from golem_blender_app.commands import utils


def has_pending_subtasks(work_dir: Path) -> bool:
    with utils.get_db_connection(work_dir) as db:
        return utils.get_next_pending_subtask(db) is not None
