import json
import struct
import zlib
from unittest.mock import AsyncMock

import httpx
import pytest

from werender.core.job import RenderJob, TaskStatus
from werender.network.coordinator import CoordinatorServer, WorkerInfo
from werender.network.discovery import NodeInfo
from werender.network.worker import TaskReportTimeout, WorkerNode


def _png_chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))


PNG = (
    b"\x89PNG\r\n\x1a\n"
    + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    + _png_chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00"))
    + _png_chunk(b"IEND", b"")
)


async def test_persistent_upload_failure_releases_retry_loop(monkeypatch):
    monkeypatch.setattr("werender.network.worker.TASK_REPORT_TIMEOUT", 0.05)
    worker = WorkerNode.__new__(WorkerNode)
    send = AsyncMock(return_value=httpx.Response(503))
    with pytest.raises(TaskReportTimeout, match="upload timed out"):
        await worker._retry_task_report(send, {"frame_number": 77}, "upload")
    send.assert_awaited()


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    coordinator = CoordinatorServer()
    job = RenderJob(name="test", blend_file=tmp_path / "test.blend", frame_start=1,
                    frame_end=2, output_directory=tmp_path / "frames")
    job.create_tasks()
    coordinator.jobs[job.id] = job
    coordinator._save_job_to_db(job)
    worker = WorkerNode.__new__(WorkerNode)
    worker.api_key = coordinator.auth_manager.get_key("worker")
    worker.coordinator = NodeInfo("coordinator", "farm.test", 80, "coordinator", {})
    first, second = job.tasks
    first.assign_to("worker")
    first.start_rendering()
    coordinator.workers["worker"] = WorkerInfo(
        worker_id="worker", worker_name="worker", cpu_cores=4, gpu_name="GPU",
        last_seen=0, current_task_id=first.id,
    )
    assignment = {"id": first.id, "frame_number": 1, "assignment": first.assigned_at.isoformat()}
    return coordinator, worker, first, second, assignment


async def test_upload_retries_after_disconnect_503_and_lost_ack(farm, tmp_path, monkeypatch):
    coordinator, worker, first, second, assignment = farm
    output = tmp_path / "render.png"
    output.write_bytes(PNG)
    sleep = AsyncMock()
    monkeypatch.setattr("werender.network.worker.asyncio.sleep", sleep)
    seen = []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=coordinator.app)) as api:
        async def deliver(request):
            body = await request.aread()
            assert PNG in body
            seen.append(body)
            if len(seen) == 1:
                raise httpx.WriteError("", request=request)
            if len(seen) == 2:
                return httpx.Response(503)
            response = await api.send(request)
            assert response.status_code == 200
            if len(seen) == 3:
                assert first.status == TaskStatus.COMPLETED
                assert coordinator.workers["worker"].current_task_id is None
                second.assign_to("worker")
                second.start_rendering()
                coordinator.workers["worker"].current_task_id = second.id
                first.output_file.write_bytes(b"keep committed bytes")
                raise httpx.ReadError("lost acknowledgement", request=request)
            return response

        async with httpx.AsyncClient(transport=httpx.MockTransport(deliver)) as worker.http_client:
            await worker._upload_result("http://farm.test", assignment, output, 1.0)
    assert len(seen) == 4
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4]
    assert first.output_file.read_bytes() == b"keep committed bytes"
    assert coordinator.workers["worker"].current_task_id == second.id


async def test_old_upload_cannot_complete_new_assignment(farm):
    coordinator, worker, first, _, assignment = farm
    first.reset()
    first.assign_to("new-worker")
    first.start_rendering()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=coordinator.app),
                                 base_url="http://farm.test") as api:
        response = await api.post(f"/api/tasks/{first.id}/complete",
                                  headers={"X-API-Key": worker.api_key},
                                  files={"output": ("frame.png", b"obsolete")},
                                  data={"render_time": "1", "assignment": assignment["assignment"]})
    assert response.status_code == 409
    assert first.status == TaskStatus.RENDERING
    assert first.output_file is None


async def test_permanent_rejection_reports_failure_and_frees_worker(farm, tmp_path, monkeypatch):
    coordinator, worker, first, _, assignment = farm
    output = tmp_path / "render.png"
    output.write_bytes(PNG)
    monkeypatch.setattr("werender.network.worker.asyncio.sleep", AsyncMock())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=coordinator.app)) as api:
        reports = []

        async def deliver(request):
            if request.url.path.endswith("/complete"):
                return httpx.Response(413)
            reports.append(json.loads(await request.aread()))
            if len(reports) == 1:
                raise httpx.ConnectError("offline", request=request)
            return await api.send(request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(deliver)) as worker.http_client:
            await worker._upload_result("http://farm.test", assignment, output, 1.0)
    assert len(reports) == 2
    assert first.status == TaskStatus.PENDING
    assert first.failures == 1
    assert first.error_message == "Upload refused: HTTP 413"
    assert coordinator.workers["worker"].current_task_id is None
