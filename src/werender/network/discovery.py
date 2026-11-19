"""Zeroconf/mDNS discovery for WeRender."""

import asyncio
import inspect
import logging
import socket
import threading
import time
from typing import Callable, Optional

import httpx
from zeroconf import ServiceBrowser, ServiceInfo, ServiceStateChange, Zeroconf

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_werender._tcp.local."
SERVICE_NAME_BASE = "WeRender"


class NodeInfo:
    """Information about a discovered node."""

    def __init__(
        self,
        name: str,
        address: str,
        port: int,
        node_type: str,
        properties: dict,
    ):
        self.name = name
        self.address = address
        self.port = port
        self.node_type = node_type
        self.properties = properties
        self.last_seen = time.time()

    @property
    def node_id(self) -> str:
        return self.properties.get("node_id", self.name)

    @property
    def hostname(self) -> str:
        return self.properties.get("hostname", "")

    @property
    def base_url(self) -> str:
        return str(httpx.URL(scheme="http", host=self.address.strip("[]"), port=self.port))

    @property
    def blender_version(self) -> str:
        return self.properties.get("blender_version", "")

    @property
    def cpu_cores(self) -> int:
        return int(self.properties.get("cpu_cores", 0))

    @property
    def gpu_name(self) -> str:
        return self.properties.get("gpu_name", "")

    def __repr__(self) -> str:
        return (
            f"NodeInfo(name={self.name}, type={self.node_type}, "
            f"address={self.address}:{self.port})"
        )


class DiscoveryService:
    """Service for advertising and discovering WeRender nodes."""

    def __init__(self, node_type: str, port: int, node_id: str, properties: dict):
        self.node_type = node_type
        self.port = port
        self.node_id = node_id
        self.properties = properties
        self.zeroconf: Optional[Zeroconf] = None
        self.service_info: Optional[ServiceInfo] = None
        self.browser: Optional[ServiceBrowser] = None
        self.discovered_nodes: dict[str, NodeInfo] = {}
        self.on_discovery: Optional[Callable[[NodeInfo], None]] = None
        self.on_removal: Optional[Callable[[NodeInfo], None]] = None
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._generation = 0

    def start(self) -> None:
        with self._lifecycle_lock:
            if self.zeroconf is None:
                try:
                    self._event_loop = asyncio.get_running_loop()
                except RuntimeError:
                    self._event_loop = None
                self._start_sync()

    def _start_sync(self) -> None:
        self.zeroconf = Zeroconf()
        service_name = f"{SERVICE_NAME_BASE}-{self.node_type}-{self.node_id}.{SERVICE_TYPE}"
        properties_dict = {
            "node_id": self.node_id.encode(),
            "node_type": self.node_type.encode(),
            "hostname": socket.gethostname().encode(),
            **{k: str(v).encode() for k, v in self.properties.items()},
        }
        local_ip = self._get_local_ip()
        self.service_info = ServiceInfo(
            SERVICE_TYPE,
            name=service_name,
            addresses=[socket.inet_aton(local_ip)],
            port=self.port,
            properties=properties_dict,
        )
        try:
            self.zeroconf.register_service(self.service_info)
            self.browser = ServiceBrowser(
                self.zeroconf,
                SERVICE_TYPE,
                handlers=[self._on_service_state_change],
            )
        except Exception as e:
            logger.warning("Error starting discovery service: %s", e)
            self.stop()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._generation += 1
            browser, info, zeroconf = self.browser, self.service_info, self.zeroconf
            self.browser = None
            self.service_info = None
            self.zeroconf = None
            try:
                if browser:
                    browser.cancel()
                if info and zeroconf:
                    zeroconf.unregister_service(info)
            finally:
                if zeroconf:
                    zeroconf.close()
                with self._lock:
                    self.discovered_nodes.clear()

    def _on_service_state_change(
        self,
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if zeroconf is not self.zeroconf:
            return
        if state_change in (ServiceStateChange.Added, ServiceStateChange.Updated):
            info = zeroconf.get_service_info(service_type, name)
            if info and zeroconf is self.zeroconf:
                self._handle_service_added(info)
        elif state_change is ServiceStateChange.Removed:
            self._handle_service_removed(name)

    def _handle_service_added(self, info: ServiceInfo) -> None:
        if self.service_info and info.name == self.service_info.name:
            return
        properties = {}
        if info.properties:
            for key, value in info.properties.items():
                try:
                    properties[key.decode()] = value.decode()
                except Exception as e:
                    logger.debug(f"Failed to decode property {key!r}: {e}")
        if properties.get("node_type") == self.node_type:
            return
        addresses = info.parsed_addresses()
        if not addresses:
            return
        address = addresses[0]
        node_type = properties.get("node_type", "unknown")
        node_info = NodeInfo(
            name=info.name,
            address=address,
            port=info.port,
            node_type=node_type,
            properties=properties,
        )
        with self._lock:
            previous = self.discovered_nodes.get(node_info.node_id)
            if previous and (previous.name, previous.address, previous.port, previous.properties) == (
                node_info.name, node_info.address, node_info.port, node_info.properties
            ):
                previous.last_seen = time.time()
                return
            self.discovered_nodes[node_info.node_id] = node_info
        self._notify(self.on_discovery, node_info)

    def _handle_service_removed(self, name: str) -> None:
        with self._lock:
            node_to_remove = None
            for node_id, node_info in self.discovered_nodes.items():
                if node_info.name == name:
                    node_to_remove = node_info
                    break
            if node_to_remove:
                del self.discovered_nodes[node_to_remove.node_id]
        if node_to_remove:
            self._notify(self.on_removal, node_to_remove)

    def _notify(self, callback, node: NodeInfo) -> None:
        if not callback:
            return
        generation = self._generation

        async def wait_for_callback(result):
            try:
                await result
            except Exception:
                logger.exception("Discovery callback failed for %s", node.name)

        def invoke():
            if generation != self._generation or not self.zeroconf:
                return
            try:
                result = callback(node)
                if inspect.isawaitable(result):
                    if self._event_loop is None:
                        if inspect.iscoroutine(result):
                            result.close()
                        logger.error("Async discovery callbacks require discovery to start in an event loop")
                    else:
                        self._event_loop.create_task(wait_for_callback(result))
            except Exception:
                logger.exception("Discovery callback failed for %s", node.name)

        if self._event_loop is None:
            invoke()
        else:
            try:
                self._event_loop.call_soon_threadsafe(invoke)
            except RuntimeError:
                pass  # The event loop has already shut down.

    def get_nodes(self, node_type: Optional[str] = None) -> list[NodeInfo]:
        with self._lock:
            nodes = list(self.discovered_nodes.values())
            if node_type:
                nodes = [n for n in nodes if n.node_type == node_type]
            return nodes

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
