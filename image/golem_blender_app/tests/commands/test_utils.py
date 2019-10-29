# pylint: disable=redefined-outer-name
import shutil
import sqlite3

import pytest

from golem_blender_app.commands.utils import (
    discard_subtask,
    get_next_pending_subtask,
    get_subtasks_statuses,
    init_tables,
    start_subtask,
    SubtaskStatus,
    update_subtask_status)


@pytest.yield_fixture
def db(tmpdir):
    conn = sqlite3.connect(str(tmpdir / 'task.db'))
    try:
        yield conn
    finally:
        conn.close()
        shutil.rmtree(tmpdir)


class TestDatabase:

    def test_init_tables(self, db):
        subtask_count = 4
        init_tables(db, subtask_count)

        cursor = db.cursor()
        cursor.execute('SELECT num, id FROM subtasks ORDER BY num ASC')
        rows = cursor.fetchall()

        assert len(rows) == subtask_count
        assert all(rows[i] == (i, None) for i in range(subtask_count))

        cursor = db.cursor()
        cursor.execute('SELECT * FROM history')
        rows = cursor.fetchall()

        assert not rows

    def test_start_subtask(self, db):
        subtask_id = 'subtask'
        init_tables(db, 1)

        start_subtask(db, 0, subtask_id)
        statuses = get_subtasks_statuses(db, [0])

        assert statuses == {0: (SubtaskStatus.COMPUTING, subtask_id)}
        assert get_next_pending_subtask(db) is None

        cursor = db.cursor()
        cursor.execute('SELECT id, num, status FROM history')
        rows = cursor.fetchall()

        assert len(rows) == 1
        assert rows[0] == (subtask_id, 0, SubtaskStatus.COMPUTING.value)

    def test_start_subtask_twice(self, db):
        init_tables(db, 1)
        start_subtask(db, 0, 'subtask')

        with pytest.raises(RuntimeError):
            start_subtask(db, 0, 'subtask_2')

    def test_start_subtask_duplicate_id(self, db):
        init_tables(db, 2)
        start_subtask(db, 0, 'subtask')

        with pytest.raises(sqlite3.IntegrityError):
            start_subtask(db, 1, 'subtask')

    def test_start_discarded_subtask(self, db):
        init_tables(db, 1)

        start_subtask(db, 0, 'subtask_1')
        discard_subtask(db, 'subtask_1')
        start_subtask(db, 0, 'subtask_2')

        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.COMPUTING, 'subtask_2')}

    def test_get_next_pending_subtask_twice(self, db):
        init_tables(db, 1)

        assert get_next_pending_subtask(db) == 0
        assert get_next_pending_subtask(db) == 0

    def test_discard_subtask(self, db):
        subtask_id = 'subtask'
        init_tables(db, 1)

        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.WAITING, None)}
        assert get_next_pending_subtask(db) == 0

        start_subtask(db, 0, subtask_id)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.COMPUTING, subtask_id)}
        assert get_next_pending_subtask(db) is None

        discard_subtask(db, subtask_id)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.WAITING, None)}
        assert get_next_pending_subtask(db) == 0

    def test_get_subtasks(self, db):
        subtask_count = 4
        init_tables(db, subtask_count)

        nums = list(range(subtask_count))
        statuses = get_subtasks_statuses(db, nums)

        assert len(statuses) == subtask_count
        for i in range(subtask_count):
            assert i in statuses
            assert statuses[i] == (SubtaskStatus.WAITING, None)

    def test_get_subtasks_invalid_nums(self, db):
        subtask_count = 4
        init_tables(db, subtask_count)

        nums = [9, 10, 11]
        statuses = get_subtasks_statuses(db, nums)
        assert not statuses

    def test_update_subtask(self, db):
        subtask_id = 'subtask_0'
        init_tables(db, 1)

        start_subtask(db, 0, subtask_id)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.COMPUTING, subtask_id)}

        update_subtask_status(db, subtask_id, SubtaskStatus.VERIFYING)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.VERIFYING, subtask_id)}

        update_subtask_status(db, subtask_id, SubtaskStatus.FINISHED)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.FINISHED, subtask_id)}

    def test_update_subtask_without_starting(self, db):
        subtask_id = 'subtask'
        init_tables(db, 2)

        update_subtask_status(db, subtask_id, SubtaskStatus.VERIFYING)
        statuses = get_subtasks_statuses(db, [1])
        assert statuses[1] == (SubtaskStatus.WAITING, None)
