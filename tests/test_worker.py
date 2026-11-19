"""Exercise worker behavior without Blender, mDNS, or a real HTTP server."""

import asyncio
import hashlib
import io
import json
import zipfile
from datetime import datetime
from unittest.mock import AsyncMock, Mock, call
from uuid import UUID

import httpx
import pytest

from werender.core.blender import BlenderRenderer, RenderResult
from werender.network import worker as worker_module
from werender.network.discovery import DiscoveryService, NodeInfo
from werender.network.worker import WorkerNode

BLEND = b"test blend bytes"
IMAGE = b"test image bytes"
DIGEST = hashlib.sha256(BLEND).hexdigest()
TASK = {
    "id": "task-001", "job_id": "job-001", "frame_number": 1,
    "blend_file_hash": DIGEST, "assignment": "lease-1",
    "output_format": "PNG", "render_settings": {"samples": 16},
}
REQUEST = "/api/tasks/request"
BLEND_URL = "/api/jobs/job-001/blend"
COMPLETE = "/api/tasks/task-001/complete"
FAIL = "/api/tasks/task-001/fail"
MANIFEST = "/api/sync/manifest"
SETTINGS = "/api/sync/settings"
ADDONS = "/api/sync/addons"
KEY = "test-key"


@pytest.fixture
def make_worker(monkeypatch, tmp_path):
    specs = Mock(hostname="test-host", cpu_cores=8, cpu_threads=16, ram_gb=16,
                 gpu_name="Test GPU", gpu_vram_gb=8)
    renderer = Mock(spec=BlenderRenderer)
    renderer.get_version.return_value = "4.0.0"
    discovery = Mock(spec=DiscoveryService)
    discovery.get_nodes.return_value = []
    monkeypatch.setattr(worker_module, "BlenderRenderer", lambda: renderer)
    monkeypatch.setattr(worker_module, "DiscoveryService", Mock(return_value=discovery))
    monkeypatch.setattr("werender.utils.system.get_system_specs", lambda: specs)
    monkeypatch.setattr("socket.gethostname", lambda: "test-host")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    for var in ("WERENDER_WORKER_NAME", "WERENDER_GPU_NAME", "WERENDER_COORDINATOR_HOST",
                "WERENDER_COORDINATOR_PORT"):
        monkeypatch.delenv(var, raising=False)
    instances = []

    def build(**kwargs):
        node = WorkerNode(**kwargs)
        instances.append(node)
        return node

    yield build, renderer, discovery, specs
    for node in instances:
        node._workspace.cleanup()


@pytest.fixture
def coordinator():
    return NodeInfo("coord", "192.0.2.10", 8420, "coordinator", {"node_id": "coord-1"})


@pytest.fixture
def other_coordinator():
    return NodeInfo("other", "192.0.2.11", 8420, "coordinator", {"node_id": "coord-2"})


@pytest.fixture
def worker(make_worker, coordinator):
    build, _, _, _ = make_worker
    node = build(name="test-worker")
    node.api_key = KEY
    node.coordinator = coordinator
    return node


@pytest.fixture
async def serve(worker):
    """Route real HTTPX requests in process; fail explicitly on unrouted traffic."""
    routes = {}
    requests = []

    async def respond(request):
        requests.append(request)
        assert request.headers.get("X-API-Key") == KEY
        key = request.method, request.url.path
        if key not in routes:
            pytest.fail(f"Unexpected request: {key}")
        response = routes[key]
        if isinstance(response, list):
            if not response:
                pytest.fail(f"Unexpected extra request: {key}")
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        worker.http_client = client
        yield routes, requests


@pytest.fixture
def zero_backoff(monkeypatch):
    # Keep wait_for and task cancellation real; skip only worker retry delays.
    monkeypatch.setattr(worker_module, "asyncio", AsyncioWithoutDelay())


class AsyncioWithoutDelay:
    def __init__(self):
        self.sleep = AsyncMock()

    def __getattr__(self, name):
        return getattr(asyncio, name)


def test_initialization(make_worker, tmp_path):
    build, renderer, discovery, specs = make_worker
    node = build()
    assert node.name == "test-host"
    assert node.port == 8421
    assert UUID(node.node_id).version == 4
    assert node.specs is specs
    assert node.blender is renderer
    assert node.blender_version == "4.0.0"
    assert node.discovery is discovery
    advertised = worker_module.DiscoveryService.call_args.kwargs
    assert advertised["node_id"] == node.node_id
    assert advertised["properties"]["blender_version"] == "4.0.0"
    assert node.coordinator is node.http_client is node.api_key is None
    assert node.is_running is node.is_synced is False
    assert node.settings == {}
    assert node.installed_addons == set()
    assert node.temp_dir.is_dir()
    assert node.config_dir == tmp_path / ".werender"
    assert node.config_dir.is_dir()
    assert node.addons_dir == node.config_dir / "addons"
    assert node.addons_dir.is_dir()


def test_custom_identity_and_missing_blender(make_worker):
    build, renderer, _, _ = make_worker
    renderer.get_version.side_effect = RuntimeError("not installed")
    node = build(name="custom-worker", port=9000)
    assert (node.name, node.port, node.blender_version) == ("custom-worker", 9000, "")


def test_discovery_pins_current_coordinator_and_switches_on_removal(worker, coordinator,
                                                              other_coordinator):
    worker.coordinator = None
    worker._on_coordinator_removed(coordinator)
    assert worker.coordinator is None
    worker.is_synced = True
    worker._on_coordinator_discovered(coordinator)
    assert worker.coordinator is coordinator
    assert not worker.is_synced
    worker.is_synced = True
    worker._on_coordinator_discovered(other_coordinator)
    worker._on_coordinator_removed(other_coordinator)
    assert worker.coordinator is coordinator
    assert worker.is_synced
    worker.discovery.get_nodes.return_value = [other_coordinator]
    worker._on_coordinator_removed(coordinator)
    assert worker.coordinator is other_coordinator
    assert not worker.is_synced
    worker.discovery.get_nodes.return_value = []
    worker._on_coordinator_removed(other_coordinator)
    assert worker.coordinator is None


@pytest.mark.parametrize("outcome", [None, 204, 500, "network"])
async def test_no_task_does_not_render(worker, serve, outcome, zero_backoff, coordinator):
    routes, requests = serve
    if outcome is None:
        worker.coordinator = None
    elif outcome == "network":
        routes["GET", REQUEST] = httpx.ConnectError("unavailable")
    else:
        routes["GET", REQUEST] = httpx.Response(outcome)

    await worker._request_and_render_task()

    worker.blender.render_frame.assert_not_called()
    if outcome is None:
        assert requests == []
    else:
        request, = requests
        assert request.url.params["worker_id"] == worker.node_id
        assert request.url.params["blender_version"] == worker.blender_version
        assert worker.coordinator is coordinator


def route_blend(routes, outcome="success"):
    routes["HEAD", BLEND_URL] = httpx.Response(200, headers={"Content-Length": str(len(BLEND))})
    routes["GET", BLEND_URL] = {
        "success": httpx.Response(200, content=BLEND),
        "not-found": httpx.Response(404),
        "network": httpx.ConnectError("download failed"),
    }[outcome]


@pytest.mark.parametrize("outcome", ["success", "not-found", "render-error"])
async def test_full_task_workflow(worker, serve, outcome):
    routes, requests = serve
    settings = {"render_quality": "high"}
    route_sync(routes, settings)
    routes["GET", REQUEST] = httpx.Response(200, json=TASK)
    routes["POST", "/api/heartbeat"] = httpx.Response(200)
    routes["POST", COMPLETE if outcome == "success" else FAIL] = httpx.Response(200)
    route_blend(routes, "not-found" if outcome == "not-found" else "success")
    rendered_dirs = []

    def render(blend_path, frame, output_dir, **kwargs):
        assert blend_path.read_bytes() == BLEND
        assert frame == TASK["frame_number"]
        assert kwargs == {"output_format": "PNG", "render_settings": TASK["render_settings"]}
        rendered_dirs.append(output_dir)
        if outcome == "render-error":
            return RenderResult(success=False, frame=frame, error_message="Render failed")
        output_file = output_dir / "0001.png"
        output_file.write_bytes(IMAGE)
        return RenderResult(success=True, frame=frame, output_file=output_file, render_time=1.5)

    worker.blender.render_frame.side_effect = render
    await worker._sync_with_coordinator()
    assert worker.is_synced
    assert worker.settings == settings
    await asyncio.wait_for(worker._request_and_render_task(), timeout=2)

    traffic = [req for req in requests if req.url.path != "/api/heartbeat"]
    expected = COMPLETE if outcome == "success" else FAIL
    assert [(r.method, r.url.path) for r in traffic] == [
        ("GET", MANIFEST), ("GET", SETTINGS), ("GET", ADDONS),
        ("GET", REQUEST), ("HEAD", BLEND_URL), ("GET", BLEND_URL), ("POST", expected),
    ]
    report = traffic[-1]
    if outcome == "success":
        assert all(part in report.content for part in
                   (b'name="assignment"', b"lease-1", b"0001.png", IMAGE, b"1.5"))
    else:
        expected_error = ("Failed to download blend file" if outcome == "not-found"
                          else "Render failed")
        assert httpx_json(report) == {"error": expected_error, "assignment": "lease-1"}
    if outcome == "not-found":
        worker.blender.render_frame.assert_not_called()
    else:
        worker.blender.render_frame.assert_called_once()
        output_dir, = rendered_dirs
        assert not output_dir.exists()


def httpx_json(request):
    assert request.headers["content-type"] == "application/json"
    return json.loads(request.content)


@pytest.mark.parametrize("cached", [None, BLEND, b"!" * len(BLEND)])
async def test_download_checks_digest_before_reusing_cache(worker, serve, cached):
    routes, requests = serve
    path = worker.temp_dir / "blend_files" / f"{DIGEST}.blend"
    if cached is not None:
        path.parent.mkdir()
        path.write_bytes(cached)
    if cached != BLEND:
        route_blend(routes)
    result = await worker._download_blend_file(worker.coordinator.base_url, "job-001", DIGEST)
    assert result == path
    assert result.read_bytes() == BLEND
    assert [request.method for request in requests] == ([] if cached == BLEND else ["HEAD", "GET"])


@pytest.mark.parametrize("outcome", ["not-found", "network"])
async def test_failed_download_publishes_no_cache_file(worker, serve, outcome):
    routes, requests = serve
    route_blend(routes, outcome)
    result = await worker._download_blend_file(worker.coordinator.base_url, "job-001", DIGEST)
    assert result is None
    assert [r.method for r in requests] == ["HEAD", "GET"]
    assert list((worker.temp_dir / "blend_files").iterdir()) == []


@pytest.mark.parametrize("transient", [None, 500, "network"])
async def test_upload_reopens_file_on_retry(worker, serve, tmp_path, transient, zero_backoff):
    routes, requests = serve
    output = tmp_path / "0001.png"
    output.write_bytes(IMAGE)
    responses = [httpx.Response(200)]
    if transient is not None:
        responses.insert(0, httpx.Response(transient) if isinstance(transient, int)
                         else httpx.ConnectError("interrupted upload"))
    routes["POST", COMPLETE] = responses
    await asyncio.wait_for(worker._upload_result(worker.coordinator.base_url, TASK, output, 1.5), 2)
    assert len(requests) == (1 if transient is None else 2)
    assert all(IMAGE in r.content and b"lease-1" in r.content for r in requests)


@pytest.mark.parametrize("kind", ["missing", "directory", "none"])
async def test_unreadable_output_is_reported(worker, serve, tmp_path, kind):
    routes, requests = serve
    routes["POST", FAIL] = httpx.Response(200)
    output = {"missing": tmp_path / "missing.png", "directory": tmp_path, "none": None}[kind]
    await worker._upload_result(worker.coordinator.base_url, TASK, output, 1.5)
    report, = requests
    assert report.url.path == FAIL
    payload = httpx_json(report)
    assert payload["error"].startswith("Cannot read output:")
    assert payload["assignment"] == TASK["assignment"]


@pytest.mark.parametrize("outcome", ["success", "empty", "network", "no-coordinator"])
async def test_report_failure(worker, serve, outcome, zero_backoff):
    routes, requests = serve
    if outcome == "no-coordinator":
        worker.coordinator = None
    elif outcome == "network":
        routes["POST", FAIL] = [httpx.ConnectError("outage"), httpx.Response(200)]
    else:
        routes["POST", FAIL] = httpx.Response(200)
    message = "" if outcome == "empty" else "Render failed"
    await asyncio.wait_for(worker._report_task_failure(TASK, message), 2)
    assert len(requests) == {"no-coordinator": 0, "network": 2}.get(outcome, 1)
    assert all(httpx_json(r) == {"error": message, "assignment": "lease-1"} for r in requests)


def route_sync(routes, settings, addons=()):
    from werender.network.sync import addons_manifest_hash

    routes["GET", MANIFEST] = httpx.Response(200, json={
        "timestamp": datetime(2026, 11, 19, 12).isoformat(),
        "settings_hash": "abc", "addons_hash": addons_manifest_hash(addons),
    })
    routes["GET", SETTINGS] = httpx.Response(200, json=settings)
    routes["GET", ADDONS] = httpx.Response(200, json=list(addons))


@pytest.mark.parametrize("outcome", ["success", "500", "network", "no-coordinator"])
async def test_sync_manifest_and_settings(worker, serve, outcome, zero_backoff):
    routes, requests = serve
    settings = {"render_quality": "high"}
    if outcome == "no-coordinator":
        worker.coordinator = None
    elif outcome == "network":
        routes["GET", MANIFEST] = httpx.ConnectError("offline")
    elif outcome == "500":
        routes["GET", MANIFEST] = httpx.Response(500)
    else:
        route_sync(routes, settings)

    await worker._sync_with_coordinator()

    assert worker.is_synced is (outcome == "success")
    assert worker.settings == (settings if outcome == "success" else {})
    expected = {"success": [MANIFEST, SETTINGS, ADDONS], "no-coordinator": []}
    assert [r.url.path for r in requests] == expected.get(outcome, [MANIFEST])


@pytest.mark.parametrize("network_error", [False, True])
async def test_settings_error_preserves_previous_settings(worker, serve, network_error, zero_backoff):
    routes, requests = serve
    worker.settings = {"previous": "value"}
    route_sync(routes, {})
    routes["GET", SETTINGS] = (httpx.ConnectError("offline") if network_error
                              else httpx.Response(500))
    await worker._sync_with_coordinator()
    assert not worker.is_synced
    assert worker.settings == {"previous": "value"}
    assert [r.url.path for r in requests] == [MANIFEST, SETTINGS]


@pytest.fixture
def addon_zip():
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("new_addon/__init__.py", "# inert test fixture")
    return data.getvalue()


@pytest.mark.parametrize("cached", ["absent", "valid", "same-size-corrupt"])
async def test_sync_addons(worker, serve, addon_zip, cached):
    routes, requests = serve
    target = worker.addons_dir / "new_addon.zip"
    if cached != "absent":
        target.write_bytes(addon_zip if cached == "valid" else b"x" * len(addon_zip))
    addon = {"name": "new_addon", "filename": target.name, "size": len(addon_zip),
             "hash": hashlib.md5(addon_zip).hexdigest()}
    route_sync(routes, {}, [addon])
    download = f"{ADDONS}/new_addon/download"
    routes["GET", download] = httpx.Response(200, content=addon_zip)
    await worker._sync_with_coordinator()
    assert worker.is_synced
    assert [r.url.path for r in requests] == [MANIFEST, SETTINGS, ADDONS] + ([] if cached == "valid" else [download])
    assert target.read_bytes() == addon_zip
    assert list(worker.addons_dir.iterdir()) == [target]


@pytest.mark.parametrize("failure", ["list", "manifest-changed", "404", "offline", "checksum", "short", "invalid-zip"])
async def test_addon_sync_failure_preserves_existing_cache(worker, serve, addon_zip, failure, zero_backoff):
    routes, _ = serve
    path = f"{ADDONS}/new_addon/download"
    target = worker.addons_dir / "new_addon.zip"
    target.write_bytes(b"previous cache")
    advertised = b"x" * len(addon_zip) if failure == "invalid-zip" else addon_zip
    addon = {"name": "new_addon", "filename": target.name, "size": len(advertised),
             "hash": hashlib.md5(advertised).hexdigest()}
    route_sync(routes, {"new": True}, [addon])
    if failure == "list":
        routes["GET", ADDONS] = httpx.Response(503)
    elif failure == "manifest-changed":
        routes["GET", ADDONS] = httpx.Response(200, json=[])
    else:
        routes["GET", path] = {
            "404": httpx.Response(404), "offline": httpx.ConnectError("offline"),
            "checksum": httpx.Response(200, content=b"x" * len(addon_zip)),
            "short": httpx.Response(200, content=addon_zip[:-1]),
            "invalid-zip": httpx.Response(200, content=advertised),
        }[failure]
    await worker._sync_with_coordinator()
    assert not worker.is_synced
    assert worker.settings == {}
    assert target.read_bytes() == b"previous cache"
    assert list(worker.addons_dir.iterdir()) == [target]

    route_sync(routes, {"new": True}, [dict(addon, hash=hashlib.md5(addon_zip).hexdigest())])
    routes["GET", path] = httpx.Response(200, content=addon_zip)
    await worker._sync_with_coordinator()
    assert worker.is_synced
    assert worker.settings == {"new": True}
    assert target.read_bytes() == addon_zip


async def test_sync_for_removed_coordinator_does_not_mark_replacement_synced(
    worker, serve, coordinator, other_coordinator,
):
    routes, _ = serve
    route_sync(routes, {})
    worker.discovery.get_nodes.return_value = [other_coordinator]

    async def replace_coordinator(_base_url, _addons_hash):
        worker._on_coordinator_removed(coordinator)

    worker._sync_addons = AsyncMock(side_effect=replace_coordinator)
    await worker._sync_with_coordinator()
    assert worker.coordinator is other_coordinator
    assert not worker.is_synced


@pytest.mark.parametrize("state,delay", [("idle", 2), ("unsynced", 1), ("synced", 1), ("error", 5)])
async def test_worker_loop_owns_client_and_dispatches(worker, monkeypatch, state, delay):
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(500)))
    factory = Mock(return_value=client)
    monkeypatch.setattr(worker_module.httpx, "AsyncClient", factory)
    no_delay = AsyncioWithoutDelay()
    monkeypatch.setattr(worker_module, "asyncio", no_delay)
    worker._sync_with_coordinator = AsyncMock()
    worker._request_and_render_task = AsyncMock()
    if state == "idle":
        worker.coordinator = None
    elif state == "synced":
        worker.is_synced = True
    elif state == "error":
        worker._sync_with_coordinator.side_effect = [RuntimeError("sync error"), None]

    async def stop_after_iteration(seconds):
        if state != "error" or seconds != 5:
            worker.is_running = False

    no_delay.sleep.side_effect = stop_after_iteration
    await asyncio.wait_for(worker._worker_loop(), 2)

    assert client.is_closed
    assert not worker.is_running
    assert worker.http_client is client
    factory.assert_called_once_with(timeout=90.0, trust_env=False)
    assert no_delay.sleep.await_args_list == [call(delay)] + ([call(1)] if state == "error" else [])
    assert worker._sync_with_coordinator.await_count == {"unsynced": 1, "error": 2}.get(state, 0)
    assert worker._request_and_render_task.await_count == int(state == "synced")


def test_start_loads_key_and_cleans_up_discovery(worker, monkeypatch):
    get_key = Mock(return_value=KEY)
    monkeypatch.setattr(worker_module, "get_worker_api_key", get_key)

    async def run_iteration():
        assert worker.discovery.on_discovery == worker._on_coordinator_discovered
        assert worker.discovery.on_removal == worker._on_coordinator_removed
        assert worker.api_key == KEY
        worker.discovery.start.assert_called_once()

    worker.api_key = None
    worker._worker_loop = AsyncMock(side_effect=run_iteration)
    worker.start()
    get_key.assert_called_once_with(worker.config_dir)
    worker._worker_loop.assert_awaited_once()
    worker.discovery.stop.assert_called_once()
    assert worker.discovery.on_discovery is worker.discovery.on_removal is None
    assert worker.coordinator is None
    assert not worker.is_synced


def test_stop(worker):
    worker.is_running = True
    worker.stop()
    assert not worker.is_running
    worker.discovery.stop.assert_called_once()
