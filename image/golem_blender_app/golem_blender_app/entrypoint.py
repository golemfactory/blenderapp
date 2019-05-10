from pathlib import Path
import asyncio
import json
import ssl
import sys

from grpclib.server import Server

from golem_task_api.golem_task_api_grpc import GolemAppBase
from golem_task_api.golem_task_api_pb2 import (
    CreateTaskRequest,
    CreateTaskReply,
    NextSubtaskRequest,
    NextSubtaskReply,
    ComputeRequest,
    ComputeReply,
    VerifyRequest,
    VerifyReply,
    RunBenchmarkRequest,
    RunBenchmarkReply,
)


from golem_blender_app.commands.benchmark import benchmark
from golem_blender_app.commands.compute import compute
from golem_blender_app.commands.create_task import create_task
from golem_blender_app.commands.get_subtask import get_next_subtask
from golem_blender_app.commands.verify import verify


class GolemApp(GolemAppBase):

    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir

    async def CreateTask(self, stream):
        request: CreateTaskRequest = await stream.recv_message()
        task_id = request.task_id
        task_work_dir = self.work_dir / task_id
        task_params = json.loads(request.task_params_json)
        create_task(task_work_dir, task_params)
        reply = CreateTaskReply()
        await stream.send_message(reply)

    async def NextSubtask(self, stream):
        request: NextSubtaskRequest = await stream.recv_message()
        task_id = request.task_id
        task_work_dir = self.work_dir / task_id
        subtask_id, subtask_params = get_next_subtask(task_work_dir)
        reply = NextSubtaskReply()
        reply.subtask_id = subtask_id
        reply.subtask_params_json = json.dumps(subtask_params)
        await stream.send_message(reply)

    async def Compute(self, stream):
        request: ComputeRequest = await stream.recv_message()
        task_id = request.task_id
        subtask_id = request.subtask_id
        task_work_dir = self.work_dir / task_id
        subtask_params = json.loads(request.subtask_params_json)
        compute(task_work_dir, subtask_id, subtask_params)
        reply = ComputeReply()
        await stream.send_message(reply)

    async def Verify(self, stream):
        request: VerifyRequest = await stream.recv_message()
        task_id = request.task_id
        subtask_id = request.subtask_id
        task_work_dir = self.work_dir / task_id
        success = verify(task_work_dir, subtask_id)
        reply = VerifyReply()
        reply.success = success
        await stream.send_message(reply)

    async def RunBenchmark(self, stream):
        request: RunBenchmarkRequest = await stream.recv_message()
        score = benchmark(self.work_dir)
        reply = RunBenchmarkReply()
        reply.score = score
        await stream.send_message(reply)


def spawn_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    loop = asyncio.get_event_loop()
    golem_app = GolemApp(work_dir)
    server = Server(handlers=[golem_app], loop=loop)

    # context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    # context.load_cert_chain(server_cert, server_key)
    # context.load_verify_locations(client_cert)
    context = None

    # Start server
    loop.run_until_complete(server.start('', port, ssl=context))
    print(f'Listening on port {port}...')
    return server


def run_server(work_dir: Path, port: int, server_cert=None, server_key=None, client_cert=None):  # noqa
    loop = asyncio.get_event_loop()
    server = spawn_server(work_dir, port)
    # Run until closed
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Shutting down server...')
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())
        wakeup_task.cancel()


if __name__ == '__main__':
    run_server(Path('/golem/work'), int(sys.argv[1]))
