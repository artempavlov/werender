import asyncio
import json
import shutil
import sqlite3
import threading
import time
from datetime import datetime

import httpx
import pytest

from tests.test_result_delivery import PNG
from werender.core.job import MAX_FRAME_ATTEMPTS, JobStatus, TaskStatus
from werender.network.coordinator import CoordinatorServer
from werender.scheduler import SchedulingStrategy


class FakeBlender:
    def get_version(self):
        return "4.2.0"

    def pack_resources(self, path, output_path):
        shutil.copyfile(path, output_path)
        return output_path


@pytest.fixture
def server(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr("werender.network.coordinator.BlenderRenderer", FakeBlender)
    server = CoordinatorServer()
    server.direct_only = True
    server.resource_manager.coordinator_limits.min_disk_space_gb = 0
    return server


@pytest.fixture
async def client(server):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app),
                                 base_url="http://test.local") as client:
        yield client


def auth(server, role="coordinator"):
    return {"X-API-Key": server.auth_manager.get_key(role)}


async def create(client, server, *, end=2, content=b"test", **form):
    return await client.post("/api/jobs/create", headers=auth(server),
                             files={"file": ("scene.blend", b"BLENDER_v401" + content)},
                             data=dict(frame_start=1, frame_end=end, **form))


async def control(client, server, job, action):
    return await client.post(f"/api/jobs/{job.id}/{action}", headers=auth(server))


async def request_task(client, server, worker="cpu", **params):
    return await client.get("/api/tasks/request", headers=auth(server, "worker"),
                            params=dict(worker_id=worker, worker_name=worker, cpu_cores=4,
                                        gpu_name="None", blender_version="4.2.0", **params))


async def upload(client, server, assignment, png=PNG):
    return await client.post(f"/api/tasks/{assignment['id']}/complete",
                             headers=auth(server, "worker"), files={"output": ("frame.png", png)},
                             data={"assignment": assignment["assignment"], "render_time": "1.2"})


async def test_restart_preserves_completed_metadata_and_requeues_active(client, server):
    template = await client.post("/api/templates", headers=auth(server),
                                 json={"name": "fast", "priority": 8})
    response = await create(client, server, template_id=template.json()["id"])
    job = server.jobs[response.json()["job_id"]]
    assert (await control(client, server, job, "start")).status_code == 200
    first = (await request_task(client, server)).json()
    assert (await upload(client, server, first)).status_code == 200
    second = (await request_task(client, server)).json()
    restarted = CoordinatorServer()
    restored = restarted.jobs[job.id]
    assert restored.status == JobStatus.RUNNING and restored.priority == 8
    assert restored.started_at == job.started_at and restored.blend_file_hash == job.blend_file_hash
    assert restored.packed_file == job.packed_file and restored.output_directory == job.output_directory
    assert restored.tasks[0].output_file.read_bytes() == PNG
    assert restored.tasks[0].render_time_seconds == 1.2
    assert restored.tasks[1].status == TaskStatus.PENDING
    assert not restarted.database.get_worker("cpu")["is_online"]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted.app),
                                 base_url="http://test.local") as reconnected:
        new = (await request_task(reconnected, restarted)).json()
        assert new["id"] == second["id"] and new["assignment"] != second["assignment"]
        assert (await upload(reconnected, restarted, second)).status_code == 409
        assert (await upload(reconnected, restarted, new)).status_code == 200
    assert CoordinatorServer().jobs[job.id].status == JobStatus.COMPLETED


def test_old_schema_backfills_incomplete_rows_and_preserves_paths(server, tmp_path):
    legacy = tmp_path / "legacy.blend"
    legacy.write_bytes(b"BLENDER_v401old")
    old_output = tmp_path / "legacy-output" / "0001.png"
    with sqlite3.connect(server.database.db_path) as db:
        db.execute("ALTER TABLE tasks DROP COLUMN failures")
        db.execute("ALTER TABLE tasks DROP COLUMN last_failed_assignment")
        db.execute("INSERT INTO jobs (id,name,blend_file_path,packed_file_path,frame_start,"
                   "frame_end,status,priority,created_at,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)",
                   ("legacy", "old", str(legacy), str(tmp_path / "missing.blend"), 1, 3,
                    "running", 0, datetime.now().isoformat(), '{"worker_tags":["fast"]}'))
        db.execute("INSERT INTO tasks (id,job_id,frame_number,status,output_file_path) "
                   "VALUES ('old-task','legacy',1,'completed',?)", (str(old_output),))
    restarted = CoordinatorServer()
    job = restarted.jobs["legacy"]
    assert [t.status for t in job.tasks] == [TaskStatus.COMPLETED] + [TaskStatus.PENDING] * 2
    assert job.tasks[0].output_file == old_output and job.tasks[0].failures == 0
    assert job.worker_tags == {"fast"} and job.priority == 5
    assert restarted._scene_file(job) == legacy


async def test_pause_edit_cancel_release_worker_and_retries_terminate(client, server):
    job = server.jobs[(await create(client, server)).json()["job_id"]]
    await control(client, server, job, "start")
    assignment = (await request_task(client, server)).json()
    assert (await request_task(client, server)).status_code == 204
    assert (await client.post("/api/heartbeat", params={"worker_id": "cpu"})).status_code >= 400
    assert (await client.post("/api/heartbeat", params={"worker_id": "cpu"},
                              headers=auth(server, "worker"))).status_code == 200
    assert (await control(client, server, job, "pause")).status_code == 200
    assert server.workers["cpu"].current_task_id is None
    assert (await upload(client, server, assignment)).status_code == 409
    patch = await client.patch(f"/api/jobs/{job.id}", headers=auth(server), json={"end_frame": 1})
    assert patch.status_code == 200
    await control(client, server, job, "resume")
    for attempt in range(MAX_FRAME_ATTEMPTS):
        assignment = (await request_task(client, server)).json()
        assert (await upload(client, server, assignment, PNG[:-1])).status_code == 422
        report = {"assignment": assignment["assignment"], "error": "render error"}
        url = f"/api/tasks/{assignment['id']}/fail"
        for _ in range(2):
            assert (await client.post(url, headers=auth(server, "worker"), json=report)).status_code == 200
        assert job.tasks[0].failures == attempt + 1
    assert job.status == JobStatus.FAILED
    assert CoordinatorServer().jobs[job.id].status == JobStatus.FAILED
    other = server.jobs[(await create(client, server)).json()["job_id"]]
    await control(client, server, other, "start")
    assignment = (await request_task(client, server)).json()
    assert (await control(client, server, other, "cancel")).status_code == 200
    assert server.workers["cpu"].current_task_id is None
    assert (await upload(client, server, assignment)).status_code == 409
    assert CoordinatorServer().jobs[other.id].status == JobStatus.CANCELLED


async def test_paths_limits_and_reload_are_isolated(client, server, monkeypatch):
    assert (await create(client, server, end=100_001)).status_code == 400
    a = server.jobs[(await create(client, server, content=b"one")).json()["job_id"]]
    b = server.jobs[(await create(client, server, content=b"two")).json()["job_id"]]
    assert a.blend_file != b.blend_file and a.output_directory != b.output_directory
    assert a.blend_file_hash != b.blend_file_hash
    config = {"id": a.id, "frame_start": 1, "frame_end": 2, "output_directory": str(a.output_directory)}
    config_file = ("job.json", json.dumps(config))
    assert (await client.post("/api/jobs/import", files={"file": config_file},
                              headers=auth(server))).status_code == 400
    imported = await client.post("/api/jobs/import", headers=auth(server),
                                 files={"file": config_file, "blend_file": ("scene.blend", b"BLENDER_v401c")})
    c = server.jobs[imported.json()["job_id"]]
    assert c.id not in {a.id, b.id} and c.output_directory not in {a.output_directory, b.output_directory}
    server.resource_manager.coordinator_limits.max_concurrent_jobs = 1
    assert (await control(client, server, a, "start")).status_code == 200
    assert (await control(client, server, b, "start")).status_code == 503
    assert (await control(client, server, a, "pause")).status_code == 200
    before = a.blend_file, a.output_directory, a.tasks[0].id
    restarted = CoordinatorServer()
    assert restarted.jobs[a.id].status == JobStatus.PAUSED
    response = await client.post(f"/api/jobs/{a.id}/reload_blend", headers=auth(server),
                                 files={"blend": ("scene.blend", b"BLENDER_v401new")})
    assert response.status_code == 200
    assert a.blend_file != before[0] and a.output_directory != before[1] and a.tasks[0].id != before[2]
    head = await client.head(f"/api/jobs/{a.id}/blend", headers=auth(server, "worker"))
    assert head.status_code == 200 and not head.content
    assert int(head.headers["content-length"]) == len(b"BLENDER_v401new")


async def test_packing_and_hashing_off_loop_and_committed_once(client, server, monkeypatch):
    ready = threading.Event()
    release = threading.Event()
    original = server._hash_scene
    hashes = []

    def hold(path):
        hashes.append(path)
        ready.set()
        if not release.wait(5):
            raise RuntimeError("coordinator blocked the event loop")
        return original(path)

    monkeypatch.setattr(server, "_hash_scene", hold)
    pending = asyncio.create_task(create(client, server))
    try:
        assert await asyncio.to_thread(ready.wait, 3)
        assert (await request_task(client, server)).status_code == 204
        assert not server.jobs
    finally:
        release.set()
    job = server.jobs[(await pending).json()["job_id"]]
    await control(client, server, job, "start")
    first = (await request_task(client, server)).json()
    await upload(client, server, first)
    second = (await request_task(client, server)).json()
    assert first["blend_file_hash"] == second["blend_file_hash"] and len(hashes) == 1


async def test_job_and_frames_rollback_together(client, server, monkeypatch):
    job = server.jobs[(await create(client, server)).json()["job_id"]]
    old_tasks = [task.id for task in job.tasks]

    def broken(*args, **kwargs):
        raise sqlite3.OperationalError("simulated disk error")

    monkeypatch.setattr(server.database, "save_task", broken)
    with pytest.raises(sqlite3.OperationalError):
        await client.patch(f"/api/jobs/{job.id}", headers=auth(server), json={"end_frame": 3})
    assert job.frame_end == server.database.get_job(job.id)["frame_end"] == 2
    assert [task["id"] for task in server.database.get_job_tasks(job.id)] == old_tasks


async def test_busy_poll_does_not_renew_orphaned_assignment(client, server):
    job = server.jobs[(await create(client, server)).json()["job_id"]]
    await control(client, server, job, "start")
    first = (await request_task(client, server)).json()
    worker = server.workers["cpu"]
    before = worker.last_seen
    assert (await request_task(client, server)).status_code == 204
    assert worker.last_seen == before
    worker.last_seen = time.time() - 31
    issued = (await request_task(client, server)).json()
    assert issued["assignment"] != first["assignment"]
    assert job.tasks[0].failures == 1


async def test_round_robin_skips_ineligible_workers(client, server):
    server.scheduler.strategy = SchedulingStrategy.ROUND_ROBIN
    for name in ("a", "b", "disabled"):
        assert (await request_task(client, server, name)).status_code == 204
    limits = await client.post("/api/resources/workers/disabled/limits", headers=auth(server),
                               json={"max_concurrent_tasks": 0})
    assert limits.status_code == 200
    assert (await client.post("/api/resources/workers/a/limits", headers=auth(server),
                              json={"cpu_utilization_cap": 0.8})).status_code == 400
    job = server.jobs[(await create(client, server)).json()["job_id"]]
    await control(client, server, job, "start")
    first = (await request_task(client, server, "a")).json()
    assert (await upload(client, server, first)).status_code == 200
    assert (await request_task(client, server, "a")).status_code == 204
    assert (await request_task(client, server, "b")).status_code == 200


async def test_direct_only_skips_discovery(server, monkeypatch):
    def unexpected():
        pytest.fail("Direct-only startup/shutdown touched mDNS")

    monkeypatch.setattr(server.discovery, "start", unexpected)
    monkeypatch.setattr(server.discovery, "stop", unexpected)
    async with server._lifespan(server.app):
        assert server._background_tasks


async def test_upload_rechecks_lease_after_file_io(client, server, monkeypatch):
    job = server.jobs[(await create(client, server)).json()["job_id"]]
    await control(client, server, job, "start")
    assignment = (await request_task(client, server)).json()
    staged = threading.Event()
    release = threading.Event()
    original = server._stage_png

    def hold(reader, partial):
        original(reader, partial)
        staged.set()
        if not release.wait(5):
            raise RuntimeError("coordinator blocked while staging result")

    monkeypatch.setattr(server, "_stage_png", hold)
    uploading = asyncio.create_task(upload(client, server, assignment))
    try:
        assert await asyncio.to_thread(staged.wait, 3)
        assert (await control(client, server, job, "pause")).status_code == 200
        assert server.workers["cpu"].current_task_id is None
    finally:
        release.set()
    assert (await uploading).status_code == 409
    assert not list(job.output_directory.iterdir())
    assert job.tasks[0].status == TaskStatus.PENDING
