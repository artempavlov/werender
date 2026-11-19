import ssl
from unittest.mock import Mock

import pytest

from werender.network.worker import WorkerNode, _https_config


@pytest.mark.parametrize("url", [
    "http://example.test", "https://", "https://user@example.test",
    "https://example.test/path", "https://example.test/?one=two",
    "https://example.test/#fragment",
])
def test_rejects_non_https_origins(url):
    with pytest.raises(ValueError, match="HTTPS origin"):
        _https_config(url, None, None, None)


def test_tls_context_loads_client_identity(monkeypatch):
    context = Mock(spec=ssl.SSLContext)
    create = Mock(return_value=context)
    monkeypatch.setattr(ssl, "create_default_context", create)
    origin, verify = _https_config("https://example.test:444/", "cert.pem", "key.pem", "ca.pem")
    assert (origin.host, origin.port, verify) == ("example.test", 444, context)
    create.assert_called_once_with(cafile="ca.pem")
    context.load_cert_chain.assert_called_once_with("cert.pem", keyfile="key.pem")


@pytest.mark.asyncio
async def test_direct_loop_uses_explicit_https_origin():
    node = object.__new__(WorkerNode)

    async def loop():
        assert node._direct_base_url == "https://example.test"
        assert node.is_synced

    node._worker_loop = loop
    await node._direct_loop("example.test", 443, base_url="https://example.test")
    assert node._direct_base_url is None
