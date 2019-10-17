from pathlib import Path
from typing import Dict, List, Optional, Tuple
import contextlib
import enum
import random
import sqlite3


class SubtaskStatus(enum.Enum):
    PENDING = 'pending'
    COMPUTING = 'computing'
    VERIFYING = 'verifying'
    FINISHED = 'finished'
    ABORTED = 'aborted'


def get_db_connection(work_dir: Path):
    return contextlib.closing(sqlite3.connect(str(work_dir / 'task.db')))


def init_tables(db, subtasks_count: int) -> None:
    with db:
        db.execute(
            'CREATE TABLE subtask_status(num int, status text, unique_id text)')
        values = \
            ((x, SubtaskStatus.PENDING.value) for x in range(subtasks_count))
        db.executemany(
            'INSERT INTO subtask_status(num, status) VALUES (?,?)',
            values,
        )


def update_subtask(
        db,
        subtask_num: int,
        status: SubtaskStatus,
        unique_id: Optional[str] = None) -> None:
    with db:
        db.execute(
            'UPDATE subtask_status SET status = ? WHERE num = ?',
            (status.value, subtask_num),
        )
        if unique_id:
            db.execute(
                'UPDATE subtask_status SET unique_id = ? WHERE num = ?',
                (unique_id, subtask_num),
            )


def get_subtasks_statuses(
        db,
        nums: List[int]) -> Dict[int, Tuple[SubtaskStatus, str]]:
    set_format = ','.join(['?'] * len(nums))
    cursor = db.cursor()
    cursor.execute(
        "SELECT num, status, unique_id FROM subtask_status WHERE num IN "
        f"({set_format})",
        (*nums,),
    )
    values = cursor.fetchall()
    return {
        v[0]: (SubtaskStatus(v[1]), v[2]) for v in values
    }


def get_next_pending_subtask(db) -> Optional[int]:
    cursor = db.cursor()
    cursor.execute(
        'SELECT num FROM subtask_status WHERE status = ? LIMIT 1',
        (SubtaskStatus.PENDING.value,)
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


def gen_subtask_id(subtask_num: int) -> str:
    return f'{subtask_num}-{random.random()}'


def get_subtask_num_from_id(subtask_id: str) -> int:
    return int(subtask_id.split('-')[0])


def get_scene_file_from_resources(resources: List[str]) -> Optional[str]:
    for resource in resources:
        if resource.lower().endswith('.blend'):
            return resource
    return None
