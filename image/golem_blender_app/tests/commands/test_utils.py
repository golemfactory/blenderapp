# pylint: disable=redefined-outer-name
import shutil
import sqlite3

import pytest

from golem_blender_app.commands.utils import (
    abort_task,
    get_next_pending_subtask,
    get_part_num,
    get_scene_file_from_resources,
    get_subtasks_statuses,
    init_tables,
    start_subtask,
    string_to_frames,
    update_subtask_status,
    SubtaskStatus,
)


@pytest.yield_fixture
def db(tmpdir):
    conn = sqlite3.connect(str(tmpdir / 'task.db'))
    try:
        yield conn
    finally:
        conn.close()
        shutil.rmtree(tmpdir)


class TestSubtaskUtils:

    def test_init_tables(self, db):
        subtask_count = 4
        init_tables(db, subtask_count)

        statuses = get_subtasks_statuses(db, list(range(subtask_count)))
        assert all(
            statuses[i] == (SubtaskStatus.WAITING, None)
            for i in range(subtask_count))

    def test_start_subtask(self, db):
        subtask_id = 'subtask'
        init_tables(db, 1)

        start_subtask(db, 0, subtask_id)
        statuses = get_subtasks_statuses(db, [0])

        assert statuses == {0: (SubtaskStatus.COMPUTING, subtask_id)}
        assert get_next_pending_subtask(db) is None

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
        update_subtask_status(db, 'subtask_1', SubtaskStatus.ABORTED)
        start_subtask(db, 0, 'subtask_2')

        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.COMPUTING, 'subtask_2')}

    def test_get_next_pending_subtask_twice(self, db):
        init_tables(db, 1)

        assert get_next_pending_subtask(db) == 0
        assert get_next_pending_subtask(db) == 0

    def test_get_next_pending_subtask_statuses(self, db):
        subtask_id = 'subtask'
        init_tables(db, 1)

        assert get_next_pending_subtask(db) == 0

        start_subtask(db, 0, subtask_id)  # SubtaskStatus.COMPUTING
        assert get_next_pending_subtask(db) is None

        update_subtask_status(db, subtask_id, SubtaskStatus.VERIFYING)
        assert get_next_pending_subtask(db) is None

        update_subtask_status(db, subtask_id, SubtaskStatus.FINISHED)
        assert get_next_pending_subtask(db) is None

        update_subtask_status(db, subtask_id, SubtaskStatus.ABORTED)
        assert get_next_pending_subtask(db) == 0

        update_subtask_status(db, subtask_id, SubtaskStatus.FAILED)
        assert get_next_pending_subtask(db) == 0

    def test_abort_subtask(self, db):
        subtask_id = 'subtask'
        init_tables(db, 1)

        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.WAITING, None)}
        assert get_next_pending_subtask(db) == 0

        start_subtask(db, 0, subtask_id)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.COMPUTING, subtask_id)}
        assert get_next_pending_subtask(db) is None

        update_subtask_status(db, subtask_id, SubtaskStatus.ABORTED)
        statuses = get_subtasks_statuses(db, [0])
        assert statuses == {0: (SubtaskStatus.ABORTED, subtask_id)}
        assert get_next_pending_subtask(db) == 0

    def test_abort_task(self, db):
        subtask_count = 10
        init_tables(db, subtask_count)

        for i in range(subtask_count):
            start_subtask(db, i, f'subtask_{i}')
            status = get_subtasks_statuses(db, [i])[i]
            assert status == (SubtaskStatus.COMPUTING, f'subtask_{i}')

        abort_task(db)

        for i in range(subtask_count):
            status = get_subtasks_statuses(db, [i])[i]
            assert status == (SubtaskStatus.ABORTED, f'subtask_{i}')

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

    def test_get_part_num(self, db):
        init_tables(db, 2)

        start_subtask(db, 0, 'subtask_0')
        start_subtask(db, 1, 'subtask_1')

        assert get_part_num(db, 'subtask_0') == 0
        assert get_part_num(db, 'subtask_1') == 1

        update_subtask_status(db, 'subtask_0', SubtaskStatus.ABORTED)
        update_subtask_status(db, 'subtask_1', SubtaskStatus.ABORTED)

        start_subtask(db, 0, 'subtask_2')
        start_subtask(db, 1, 'subtask_3')

        assert get_part_num(db, 'subtask_2') == 0
        assert get_part_num(db, 'subtask_3') == 1


class TestGetSceneFromResources:

    def test_empty_resources(self):
        resources = []
        assert get_scene_file_from_resources(resources) is None

    def test_invalid_resources(self):
        resources = ['file.txt', 'path/to/file.bin']
        assert get_scene_file_from_resources(resources) is None

    def test(self):
        resources = ['path/to/file.BLeNd', 'other.txt']
        assert get_scene_file_from_resources(resources) == 'path/to/file.BLeNd'


# Borrowed from core: tests.apps.rendering.task.test_framerenderingtask
class TestStringToFrames:

    def test_invalid_values(self):
        with pytest.raises(ValueError):
            string_to_frames('abc')
        with pytest.raises(ValueError):
            string_to_frames('0-15,5;abc')
        with pytest.raises(ValueError):
            string_to_frames('5-8;1-2-3')
        with pytest.raises(ValueError):
            string_to_frames('1-100,2,3')
        with pytest.raises(AttributeError):
            string_to_frames(0)

    def test_values(self):
        def values(*args):
            return list(range(*args))

        assert string_to_frames('1-4') == values(1, 5)
        assert string_to_frames('1 - 4') == values(1, 5)
        assert string_to_frames('5-8;1-3') == [1, 2, 3, 5, 6, 7, 8]
        assert string_to_frames('0-15,5;23') == [0, 5, 10, 15, 23]
        assert string_to_frames('0-9; 13-15') == values(10) + values(13, 16)
        assert string_to_frames('0-15,5;23-25;26') == [
            0, 5, 10, 15, 23, 24, 25, 26]
