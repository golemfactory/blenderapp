from pathlib import Path
import shutil

import pytest

from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.restart_task import restart_task
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.verify import verify
from golem_blender_app.commands.restart_task import restart_task

from .simulationbase import SimulationBase


@pytest.mark.skipif(
    shutil.which('blender') is None,
    reason='blender not available')
class TestCommands(SimulationBase):
    def _create_task(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        create_task(req_work, req_resources, req_net_resources)

    def _get_next_subtask(
            self,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path):
        get_next_subtask(req_work, req_resources, req_net_resources)

    def _compute(
            self,
            prov_work: Path,
            prov_net_resources: Path):
        compute(prov_work, prov_net_resources)

    def _verify(
            self,
            subtask_id: str,
            req_work: Path,
            req_resources: Path,
            req_net_resources: Path,
            req_results: Path,
            req_net_results: Path):
        verify(
            subtask_id,
            req_work,
            req_resources,
            req_net_resources,
            req_results,
            req_net_results,
        )
    def _restart_task(
            self,
            req_work: Path,
            req_results: Path):
        restart_task(
            req_work,
            req_results
        )
