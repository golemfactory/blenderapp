from pathlib import Path
from typing import Dict, List, Optional, Tuple
import contextlib
import enum
import sqlite3


class SubtaskStatus(enum.Enum):
    WAITING = None
    COMPUTING = 'computing'
    VERIFYING = 'verifying'
    FINISHED = 'finished'
    FAILED = 'failed'
    ABORTED = 'aborted'

    def is_computable(self):
        return self in (self.WAITING, self.FAILED, self.ABORTED)


def get_db_connection(work_dir: Path):
    return contextlib.closing(sqlite3.connect(str(work_dir / 'task.db')))


def init_tables(db, subtasks_count: int) -> None:
    with db:
        db.execute(
            'CREATE TABLE subtasks( '
            '   num int NOT NULL PRIMARY KEY, '
            '   id text)')
        db.execute(
            'CREATE TABLE history( '
            '   id text NOT NULL PRIMARY KEY, '
            '   num int NOT NULL, '
            '   status text NOT NULL, '
            '   created DATETIME DEFAULT CURRENT_TIMESTAMP, '
            '   FOREIGN KEY(num) REFERENCES subtasks(num))')
        db.executemany(
            'INSERT INTO subtasks(num, id) '
            'VALUES (?, ?)',
            ((x, None) for x in range(subtasks_count)),
        )


def start_subtask(
        db,
        subtask_num: int,
        subtask_id: str,
) -> None:
    status = get_subtasks_statuses(db, [subtask_num])
    if status and subtask_num in status:
        if not status[subtask_num][0].is_computable():
            raise RuntimeError(f"Subtask {subtask_num} already started")

    with db:
        db.execute(
            'INSERT INTO history(id, num, status) '
            'VALUES (?, ?, ?)',
            (subtask_id, subtask_num, SubtaskStatus.COMPUTING.value)
        )
        db.execute(
            'UPDATE subtasks '
            'SET id = ? '
            'WHERE num = ?',
            (subtask_id, subtask_num)
        )


def discard_subtask(
        db,
        subtask_id: str,
) -> None:
    with db:
        db.execute(
            'UPDATE history '
            'SET status = ? '
            'WHERE id = ? ',
            (SubtaskStatus.ABORTED.value,
             subtask_id)
        )
        db.execute(
            'UPDATE subtasks '
            'SET id = NULL '
            'WHERE id = ?',
            (subtask_id,)
        )


def update_subtask_status(
        db,
        subtask_id: str,
        status: SubtaskStatus
) -> None:
    with db:
        db.execute(
            'UPDATE history '
            'SET status = ? '
            'WHERE id = ?',
            (status.value, subtask_id),
        )


def get_subtasks_statuses(
        db,
        nums: List[int]
) -> Dict[int, Tuple[SubtaskStatus, str]]:
    set_format = ','.join(['?'] * len(nums))

    cursor = db.cursor()
    cursor.execute(
        'SELECT S.num, H.status, S.id '
        'FROM subtasks S '
        'LEFT JOIN history H ON (H.id = S.id) '
        f'WHERE S.num IN ({set_format})',
        (*nums,))

    return {
        row[0]: (SubtaskStatus(row[1]), row[2]) for row in cursor.fetchall()
    }


def get_next_pending_subtask(db) -> Optional[int]:
    cursor = db.cursor()
    cursor.execute(
        'SELECT S.num '
        'FROM subtasks S '
        'WHERE NOT EXISTS ('
        '    SELECT H.id '
        '    FROM history H '
        '    WHERE H.id = S.id '
        '    AND H.status NOT IN (?, ?)'
        ') '
        'ORDER BY S.num ASC '
        'LIMIT 1',
        (SubtaskStatus.ABORTED.value, SubtaskStatus.FAILED.value)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_subtask_num(db, subtask_id: str) -> Optional[int]:
    cursor = db.cursor()
    cursor.execute(
        'SELECT num '
        'FROM history '
        'WHERE id = ? '
        'LIMIT 1',
        (subtask_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def string_to_frames(s):
    frames = []
    after_split = s.split(";")
    for i in after_split:
        inter = i.split("-")
        if len(inter) == 1:
            # single frame (e.g. 5)
            frames.append(int(inter[0]))
        elif len(inter) == 2:
            inter2 = inter[1].split(",")
            # frame range (e.g. 1-10)
            if len(inter2) == 1:
                start_frame = int(inter[0])
                end_frame = int(inter[1]) + 1
                frames += list(range(start_frame, end_frame))
            # every nth frame (e.g. 10-100,5)
            elif len(inter2) == 2:
                start_frame = int(inter[0])
                end_frame = int(inter2[0]) + 1
                step = int(inter2[1])
                frames += list(range(start_frame, end_frame, step))
            else:
                raise ValueError("Wrong frame step")
        else:
            raise ValueError("Wrong frame range")
    return sorted(frames)


def get_scene_file_from_resources(resources: List[str]) -> Optional[str]:
    for resource in resources:
        if resource.lower().endswith('.blend'):
            return resource
    return None
