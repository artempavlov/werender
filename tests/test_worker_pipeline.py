"""Worker protocol tests with in-process HTTP responses and harmless data."""

import asyncio
import hashlib
import ipaddress
import threading
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
from zeroconf import ServiceInfo, ServiceStateChange

from werender.core.blender import BlenderRenderer, RenderResult
from werender.network.chunked_transfer import ChunkedTransfer
from werender.network.discovery import SERVICE_TYPE, DiscoveryService, NodeInfo
from werender.network.transfer import TransferOptimizer
from werender.network.worker import WorkerNode


DATA = b"plain data used to verify byte-for-byte transport"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "wrong_range", "wrong_total", "short", "long", "corrupt", "ignored"])
async def test_range_validates_response_and_digest(tmp_path, mode):
    destination = tmp_path / "test.blend"

    def respond(request):
        assert request.headers["accept-encoding"] == "identity"
        assert not destination.exists()
        start, end = map(int, request.headers["range"].removeprefix("bytes=").split("-"))
        block = DATA[start : end + 1]
        total = len(DATA)
        if mode == "ignored":
            return httpx.Response(200, content=DATA)
        if mode == "wrong_range":
            start += 1
        if mode == "wrong_total":
            total += 1
        if mode == "short":
            block = block[:-1]
        if mode == "long":
            block += b"!"
        if mode == "corrupt":
            block = b"!" + block[1:]
        return httpx.Response(206, headers={"Content-Range": f"bytes {start}-{end}/{total}"}, content=block)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        transfer = ChunkedTransfer("http://test/file", destination, chunk_size=8)
        result = await transfer.download(client, expected_hash=hashlib.sha256(DATA).hexdigest(), total_size=len(DATA))
    assert result is (mode == "valid")
    if result:
        assert destination.read_bytes() == DATA
    else:
        assert list(tmp_path.iterdir()) == []


@pytest.fixture
def protocol_worker(tmp_path):
    worker = WorkerNode.__new__(WorkerNode)
    worker.temp_dir = tmp_path
    worker.api_key = "key"
    worker.transfer_optimizer = TransferOptimizer()
    worker.bandwidth_manager = worker.transfer_optimizer.bandwidth_manager
    worker.coordinator = NodeInfo("coordinator", "test", 8420, "coordinator", {})
    worker.node_id = "worker-id"
    worker.name = "worker-name"
    worker.specs = Mock(cpu_cores=4, ram_gb=8)
    worker.gpu_name = "GPU"
    worker.blender_version = "5.2.2"
    return worker


@pytest.mark.asyncio
@pytest.mark.parametrize("digest_factory", [hashlib.md5, hashlib.sha256])
async def test_cache_rechecked_then_falls_back_once(protocol_worker, tmp_path, monkeypatch, digest_factory):
    worker = protocol_worker
    monkeypatch.setattr("werender.network.worker.CHUNKED_THRESHOLD", 1)
    digest = digest_factory(DATA).hexdigest()
    destination = tmp_path / "blend_files" / f"{digest}.blend"
    destination.parent.mkdir()
    destination.write_bytes(b"corrupt old cache")
    requests = []

    def respond(request):
        requests.append((request.method, "range" in request.headers))
        assert not destination.exists()
        return httpx.Response(200, headers={"Content-Length": str(len(DATA))}, content=DATA)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as worker.http_client:
        assert await worker._download_blend_file("http://test", "job1", digest) == destination
        assert destination.read_bytes() == DATA
        assert await worker._download_blend_file("http://test", "job2", digest) == destination
    assert requests == [("HEAD", False), ("GET", True), ("GET", False)]


@pytest.mark.asyncio
async def test_full_download_never_publishes_digest_mismatch(protocol_worker, tmp_path):
    worker = protocol_worker
    digest = hashlib.sha256(DATA).hexdigest()

    def respond(request):
        if request.method == "HEAD":
            return httpx.Response(405)
        return httpx.Response(200, content=DATA[::-1])

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as worker.http_client:
        assert await worker._download_blend_file("http://test", "job", digest) is None
    assert not list((tmp_path / "blend_files").iterdir())


@pytest.mark.asyncio
async def test_heartbeat_during_render_and_assignment_delivery(protocol_worker, monkeypatch):
    worker = protocol_worker
    monkeypatch.setattr("werender.network.worker.HEARTBEAT_INTERVAL", 0.01)
    assignment = "2026-01-01T00:00:00"
    heartbeats = []
    rendering = threading.Event()
    heartbeat_during_render = threading.Event()
    uploads = []

    async def respond(request):
        assert request.headers["x-api-key"] == "key"
        path = request.url.path
        if path == "/api/tasks/request":
            assert request.url.params["blender_version"] == "5.2.2"
            return httpx.Response(200, json={"id": "task", "job_id": "job", "frame_number": 3,
                "assignment": assignment, "blend_file_hash": hashlib.sha256(DATA).hexdigest(),
                "output_format": "EXR", "render_settings": {"samples": 8}})
        if path == "/api/heartbeat":
            heartbeats.append(request.url.params["worker_id"])
            if rendering.is_set():
                heartbeat_during_render.set()
            return httpx.Response(200)
        if path.endswith("/blend"):
            return httpx.Response(200, content=DATA)
        assert path == "/api/tasks/task/complete"
        uploads.append(await request.aread())
        return httpx.Response(200)

    def render(file, frame, directory, *, output_format, render_settings):
        assert file.read_bytes() == DATA
        assert output_format == "EXR" and render_settings == {"samples": 8}
        rendering.set()
        assert heartbeat_during_render.wait(2), "renderer blocked worker heartbeats"
        output = directory / "0003.exr"
        output.write_bytes(b"fresh output")
        return RenderResult(True, frame, output, 0.5)

    worker.blender = Mock(render_frame=render)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as worker.http_client:
        await worker._request_and_render_task()
    assert heartbeats and set(heartbeats) == {worker.node_id}
    assert heartbeat_during_render.is_set()
    assert len(uploads) == 1 and assignment.encode() in uploads[0] and b"fresh output" in uploads[0]
    assert not list(worker.temp_dir.glob("frame-*"))


@pytest.mark.asyncio
async def test_direct_worker_keeps_endpoint_after_connect_error(protocol_worker, monkeypatch):
    worker = protocol_worker
    worker.discovery = Mock()
    seen = []
    real_client = httpx.AsyncClient

    def respond(request):
        seen.append(request.url.path)
        if len(seen) == 1:
            raise httpx.ConnectError("offline", request=request)
        worker.is_running = False
        return httpx.Response(204)

    def client(**options):
        assert options["trust_env"] is False
        return real_client(transport=httpx.MockTransport(respond), **options)

    monkeypatch.setattr("werender.network.worker.httpx.AsyncClient", client)
    await asyncio.wait_for(worker._direct_loop("127.0.0.1", 8420), timeout=5)
    assert seen == ["/api/tasks/request", "/api/tasks/request"]
    worker.discovery.start.assert_not_called()


@pytest.mark.parametrize("gpu_env", ["WERENDER_GPU_SETUP", "WERENDER_CUDA_SETUP"])
def test_stale_output_is_not_reused_and_gpu_setup_precedes_render(tmp_path, monkeypatch, gpu_env):
    monkeypatch.delenv("WERENDER_GPU_SETUP", raising=False)
    monkeypatch.delenv("WERENDER_CUDA_SETUP", raising=False)
    monkeypatch.setenv(gpu_env, "/app/local-setup.py")
    renderer = BlenderRenderer.__new__(BlenderRenderer)
    renderer.blender_path = "blender"
    source = tmp_path / "source.blend"
    stale = tmp_path / "0001.png"
    stale.write_bytes(b"output from old render")

    def no_output(command, **kwargs):
        assert command.index("--disable-autoexec") < command.index(str(source))
        assert command.index(str(source)) < command.index("/app/local-setup.py") < command.index("-f")
        assert Path(command[command.index("-o") + 1]).parent != tmp_path
        return Mock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("werender.core.blender.subprocess.run", no_output)
    result = renderer.render_frame(source, 1, tmp_path)
    assert not result.success and result.output_file is None
    assert stale.read_bytes() == b"output from old render"


def test_discovery_enum_changes_and_late_callbacks():
    discovery = DiscoveryService("worker", 8421, "worker", {})
    name = f"WeRender-coordinator-x.{SERVICE_TYPE}"
    advertised = ServiceInfo(SERVICE_TYPE, name, port=8420, properties={"node_type": "coordinator", "node_id": "x"},
                            addresses=[ipaddress.ip_address("::1").packed])
    mdns = Mock(get_service_info=Mock(return_value=advertised))
    discovery.zeroconf = mdns
    discovery.on_discovery = Mock(side_effect=RuntimeError("caller failure"))
    discovery.on_removal = Mock()
    discovery._on_service_state_change(mdns, SERVICE_TYPE, name, ServiceStateChange.Added)
    assert discovery.get_nodes()[0].base_url == "http://[::1]:8420"
    discovery._on_service_state_change(mdns, SERVICE_TYPE, name, ServiceStateChange.Removed)
    assert discovery.get_nodes() == []
    assert discovery.on_removal.call_args.args[0].node_id == "x"
    discovery.stop()
    discovery._on_service_state_change(mdns, SERVICE_TYPE, name, ServiceStateChange.Added)
    discovery.on_discovery.assert_called_once()


@pytest.mark.asyncio
async def test_discovery_async_callback_dispatched_on_owner_loop(monkeypatch):
    discovery = DiscoveryService("coordinator", 8420, "coordinator", {})
    mdns = Mock()
    owner = asyncio.get_running_loop()
    notified = asyncio.Event()
    node = NodeInfo("worker", "127.0.0.1", 8421, "worker", {})

    def start():
        discovery.zeroconf = mdns

    async def callback(received):
        assert asyncio.get_running_loop() is owner
        assert received is node
        notified.set()

    monkeypatch.setattr(discovery, "_start_sync", start)
    discovery.on_discovery = callback
    discovery.start()
    try:
        await asyncio.to_thread(discovery._notify, discovery.on_discovery, node)
        await asyncio.wait_for(notified.wait(), timeout=1)
    finally:
        discovery.stop()
