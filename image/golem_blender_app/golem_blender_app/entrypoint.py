from pathlib import Path

import click

from golem_blender_app.commands.benchmark import benchmark as benchmark_impl
from golem_blender_app.commands.compute import compute as compute_impl
from golem_blender_app.commands.create_task import (
    create_task as create_task_impl
)
from golem_blender_app.commands.get_subtask import (
    get_next_subtask as get_next_subtask_impl
)
from golem_blender_app.commands.verify import verify as verify_impl
from golem_blender_app.commands.copy_task import (
    copy_task as copy_task_impl
)


WORK_DIR = Path('/golem/work')
RESOURCES_DIR = Path('/golem/resources')
NETWORK_RESOURCES_DIR = Path('/golem/network_resources')
RESULTS_DIR = Path('/golem/results')
NETWORK_RESULTS_DIR = Path('/golem/network_results')
BENCHMARK_DIR = Path('/golem/benchmark')


@click.group()
def main():
    pass


@main.command()
def create_task():
    create_task_impl(
        WORK_DIR,
        RESOURCES_DIR,
        NETWORK_RESOURCES_DIR,
    )


@main.command()
def get_next_subtask():
    get_next_subtask_impl(
        WORK_DIR,
        RESOURCES_DIR,
        NETWORK_RESOURCES_DIR,
    )


@main.command()
def compute():
    compute_impl(
        WORK_DIR,
        NETWORK_RESOURCES_DIR,
    )


@main.command()
@click.argument('subtask_id')
def verify(subtask_id: str):
    verify_impl(
        subtask_id,
        WORK_DIR,
        RESOURCES_DIR,
        NETWORK_RESOURCES_DIR,
        RESULTS_DIR,
        NETWORK_RESULTS_DIR,
    )


@main.command()
def benchmark():
    benchmark_impl(
        WORK_DIR,
        BENCHMARK_DIR,
    )


@main.command()
def copy_task():
    copy_task_impl(
        WORK_DIR,
        RESULTS_DIR,
    )


if __name__ == "__main__":
    main()
