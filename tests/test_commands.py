from pathlib import Path
import shutil

import pytest

from .simulationbase import (
    SimulationBase,
    task_lifecycle_util,
)

from golem_task_api import (
    TaskApiService,
)
from golem_task_api.testutils import InlineTaskApiService

from golem_blender_app.entrypoint import (
    ProviderHandler,
    RequestorHandler,
)


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _get_task_api_service(
            self,
            work_dir: Path,
    ) -> TaskApiService:
        return InlineTaskApiService(
            work_dir,
            provider_handler=ProviderHandler(),
            requestor_handler=RequestorHandler(),
        )
