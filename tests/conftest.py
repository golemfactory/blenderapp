from pathlib import Path

import pytest

from golem_task_api.testutils import TaskLifecycleUtil


@pytest.fixture
def task_lifecycle_util(tmpdir):
    print('workdir:', tmpdir)
    return TaskLifecycleUtil(Path(tmpdir))
