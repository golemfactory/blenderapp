import asyncio
import time

from dataclasses import dataclass
from golem_task_api.threading import Executor
from psutil import Process


@dataclass
class Usage:
    mem_peak: float = 0.  # bytes
    cpu_time: float = 0.
    real_time: float = 0.


def _monitor_pid(pid: int, usage: Usage):
    proc = Process(pid)

    while not Executor.is_shutting_down():
        if not proc.is_running():
            return
        usage.cpu_time = sum(proc.cpu_times())
        usage.mem_peak = max(usage.mem_peak, proc.memory_info().vms)
        time.sleep(0.5)


async def exec_and_monitor_cmd(cmd):
    usage = Usage()
    time_started = time.time()

    process = await asyncio.create_subprocess_exec(*cmd)
    asyncio.ensure_future(Executor.run(_monitor_pid, process.pid, usage))

    try:
        return_code = await process.wait()
    except asyncio.CancelledError:
        process.terminate()
        raise

    usage.real_time = time.time() - time_started
    return return_code, usage


async def exec_cmd(cmd):
    process = await asyncio.create_subprocess_exec(*cmd)
    try:
        return await process.wait()
    except asyncio.CancelledError:
        process.terminate()
        raise
