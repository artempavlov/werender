import importlib.util
import sys
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest


@pytest.fixture
def submitter(monkeypatch):
    # The script is shipped as a standalone add-on, not imported by the server.
    bpy = SimpleNamespace(
        types=SimpleNamespace(AddonPreferences=object, PropertyGroup=object,
                              Operator=object, Panel=object),
        props=SimpleNamespace(**{name: lambda **kwargs: None for name in
                                ("StringProperty", "IntProperty", "BoolProperty")}),
    )
    monkeypatch.setitem(sys.modules, "bpy", bpy)
    path = Path(__file__).resolve().parents[1] / "src/werender/addons/werender_submitter.py"
    spec = importlib.util.spec_from_file_location("submitter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(("status", "payload", "failure"), [
    (201, b'{"job_id": "local-job"}', None),
    (403, b'{"detail": "Invalid API key"}', "Access denied"),
    (413, b'{"detail": "File too large"}', "File too large"),
    (307, b'', "Upload redirected"),
])
def test_upload_job(submitter, tmp_path, status, payload, failure):
    scene = tmp_path / "test.blend"
    contents = b"BLENDER\x00" * 9000
    scene.write_bytes(contents)
    received = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.update(path=self.path, key=self.headers.get("X-API-Key"))
            envelope = f'Content-Type: {self.headers["Content-Type"]}\r\n\r\n'.encode()
            data = self.rfile.read(int(self.headers["Content-Length"]))
            received["parts"] = BytesParser(policy=policy.default).parsebytes(envelope + data)
            self.send_response(status)
            self.end_headers()
            self.wfile.write(payload)

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        server.timeout = 5
        thread = Thread(target=server.handle_request)
        thread.start()
        try:
            args = ("127.0.0.1", server.server_port, False, "test-key", scene, "Café", 7, 11)
            if failure:
                with pytest.raises(submitter.SubmissionError, match=failure):
                    submitter.upload_job(*args)
            else:
                assert submitter.upload_job(*args) == "local-job"
        finally:
            thread.join()

    assert received["path"] == "/api/jobs/create"
    assert received["key"] == "test-key"
    parts = {part.get_param("name", header="content-disposition"): part
             for part in received["parts"].iter_parts()}
    assert parts["file"].get_filename() == "scene.blend"
    assert parts["file"].get_payload(decode=True) == contents
    assert parts["name"].get_payload(decode=True) == "Café".encode()
    assert parts["frame_start"].get_payload(decode=True) == b"7"
    assert parts["frame_end"].get_payload(decode=True) == b"11"
