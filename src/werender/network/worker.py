"""Worker node for WeRender."""

import asyncio
import mimetypes
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Set

import httpx
from werender.core.blender import BlenderRenderer
from werender.network.discovery import DiscoveryService, NodeInfo
from werender.network.auth import get_worker_api_key
from werender.network.transfer import (
    TransferOptimizer,
    BandwidthManager,
    NetworkQualityDetector,
    TransferPriority,
)
from werender.network.chunked_transfer import ChunkedTransfer
from werender.network.security import matches_file_hash, new_file_hasher

CHUNKED_THRESHOLD = 50 * 1024 * 1024
HEARTBEAT_INTERVAL = 8
DOWNLOAD_TIMEOUT = 20 * 60
TASK_REPORT_TIMEOUT = 180


class TaskReportTimeout(Exception):
    """The coordinator could not acknowledge a task report in time."""


class WorkerNode:
    """A worker node that renders frames for a coordinator."""

    def __init__(
        self,
        name: Optional[str] = None,
        port: int = 8421,
    ):
        """
        Initialize the worker node.

        Args:
            name: Custom worker name (defaults to hostname)
            port: Port to run worker service on
        """
        import socket
        import uuid

        self.name = name or os.getenv("WERENDER_WORKER_NAME") or socket.gethostname()
        self.port = port
        self.node_id = str(uuid.uuid4())

        # Get system specs
        from werender.utils.system import get_system_specs
        self.specs = get_system_specs()
        self.gpu_name = os.getenv("WERENDER_GPU_NAME") or self.specs.gpu_name or "None"

        # Get Blender version
        self.blender = BlenderRenderer()
        try:
            self.blender_version = self.blender.get_version()
        except Exception:
            self.blender_version = ""

        # Discovery service
        self.discovery = DiscoveryService(
            node_type="worker",
            port=self.port,
            node_id=self.node_id,
            properties={
                "name": self.name,
                "hostname": socket.gethostname(),
                "cpu_cores": self.specs.cpu_cores,
                "cpu_threads": self.specs.cpu_threads,
                "ram_gb": self.specs.ram_gb,
                "gpu_name": self.gpu_name,
                "gpu_vram_gb": self.specs.gpu_vram_gb or 0,
                "blender_version": self.blender_version,
            },
        )

        # Worker state
        self.coordinator: Optional[NodeInfo] = None
        self._workspace = tempfile.TemporaryDirectory(prefix="werender-worker-")
        self.temp_dir = Path(self._workspace.name)
        
        # Sync state
        self.config_dir = Path.home() / ".werender"
        self.config_dir.mkdir(exist_ok=True)
        self.addons_dir = self.config_dir / "addons"
        self.addons_dir.mkdir(exist_ok=True)

        self.is_running = False
        self.is_synced = False
        self.settings: dict = {}
        self.installed_addons: Set[str] = set()

        # Async client for HTTP requests
        self.http_client: Optional[httpx.AsyncClient] = None

        # API key for authentication
        self.api_key = None

        # Transfer optimization
        self.bandwidth_manager = BandwidthManager(throttle_during_render=True)
        self.quality_detector = NetworkQualityDetector()
        self.transfer_optimizer = TransferOptimizer(
            bandwidth_manager=self.bandwidth_manager,
            quality_detector=self.quality_detector,
        )

    def start(self) -> None:
        """Start the worker node."""
        print("🎬 WeRender - Worker Node")
        print("=" * 50)
        print(f"📛 Name: {self.name}")
        print(f"🆔 Node ID: {self.node_id}")
        print(f"🖥️  CPU: {self.specs.cpu_cores} cores")
        print(f"💾 RAM: {self.specs.ram_gb} GB")
        if self.gpu_name != "None":
            print(f"🎮 GPU: {self.gpu_name}")
        print(f"🎨 Blender: {self.blender_version or 'Not found'}")
        print()

        # Load API key
        try:
            self.api_key = get_worker_api_key(self.config_dir)
            print("✅ API key loaded successfully")
        except ValueError as e:
            print(f"❌ Error: {e}")
            print("\nTo fix this:")
            print("1. Ensure the coordinator has been started at least once")
            print("2. Or set the WERENDER_API_KEY environment variable")
            print("3. Or copy api_keys.json from the coordinator's config directory")
            print(f"   Expected location: {self.config_dir / 'api_keys.json'}")
            print()
            return

        if address := os.getenv("WERENDER_COORDINATOR_HOST"):
            try:
                port = int(os.getenv("WERENDER_COORDINATOR_PORT", "8420"))
                if not 1 <= port <= 65535:
                    raise ValueError
            except ValueError:
                print("❌ WERENDER_COORDINATOR_PORT must be an integer from 1 to 65535")
                return
            asyncio.run(self._direct_loop(address, port))
        else:
            print("🔍 Scanning for coordinator...")
            asyncio.run(self._discovery_loop())

    def stop(self) -> None:
        """Stop the worker node."""
        print("🛑 Shutting down worker...")
        self.is_running = False
        self.discovery.stop()

    async def _worker_loop(self) -> None:
        """Main worker loop."""
        self.is_running = True
        async with httpx.AsyncClient(timeout=90.0, trust_env=False) as self.http_client:
            try:
                while self.is_running:
                    try:
                        if self.coordinator:
                            if not self.is_synced:
                                await self._sync_with_coordinator()
                            if self.coordinator and self.is_synced:
                                await self._request_and_render_task()
                            if self.is_running:
                                await asyncio.sleep(1)
                        else:
                            await asyncio.sleep(2)
                    except Exception as e:
                        print(f"❌ Error in worker loop: {e}")
                        await asyncio.sleep(5)
            finally:
                self.is_running = False

    async def _direct_loop(self, address: str, port: int) -> None:
        self.coordinator = NodeInfo("coordinator", address, port, "coordinator", {})
        self.is_synced = True  # As in existing direct deployments, skip optional mDNS sync.
        await self._worker_loop()

    async def _discovery_loop(self) -> None:
        self.discovery.on_discovery = self._on_coordinator_discovered
        self.discovery.on_removal = self._on_coordinator_removed
        try:
            self.discovery.start()
            await self._worker_loop()
        finally:
            self.discovery.on_discovery = None
            self.discovery.on_removal = None
            self.discovery.stop()
            self.coordinator = None
            self.is_synced = False

    async def _heartbeat(self, base_url: str) -> None:
        while True:
            try:
                await self.http_client.post(
                    f"{base_url}/api/heartbeat", params={"worker_id": self.node_id},
                    headers={"X-API-Key": self.api_key}, timeout=5,
                )
            except httpx.HTTPError:
                pass  # The active task uses the same retry/reassignment protocol after an outage.
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    def _on_coordinator_discovered(self, node: NodeInfo) -> None:
        """Handle coordinator discovery."""
        if node.node_type != "coordinator":
            return
        if self.coordinator and self.coordinator.node_id != node.node_id:
            return
        print(f"✅ Found coordinator: {node.hostname} ({node.address}:{node.port})")
        self.coordinator = node
        self.is_synced = False  # Reset sync state on new coordinator

    def _on_coordinator_removed(self, node: NodeInfo) -> None:
        """Handle coordinator removal."""
        if self.coordinator and self.coordinator.node_id == node.node_id:
            print(f"⚠️  Coordinator {node.hostname} disconnected")
            self.coordinator = None
            self.is_synced = False
            candidates = self.discovery.get_nodes("coordinator")
            if candidates:
                self._on_coordinator_discovered(candidates[0])

    async def _request_and_render_task(self) -> None:
        """Request a task from coordinator and render it."""
        if not self.coordinator:
            return

        base_url = self.coordinator.base_url
        task_data = None
        heartbeat = None

        try:
            # Request a task with API key
            headers = {"X-API-Key": self.api_key}
            async with self.http_client.stream(
                "GET",
                f"{base_url}/api/tasks/request",
                params={
                    "worker_id": self.node_id,
                    "worker_name": self.name,
                    "cpu_cores": self.specs.cpu_cores,
                    "ram_gb": self.specs.ram_gb,
                    "gpu_name": self.gpu_name,
                    "blender_version": self.blender_version,
                },
                headers=headers,
            ) as response:
                if response.status_code == 204:
                    # No tasks available
                    if self.is_running:
                        await asyncio.sleep(2)
                    return

                if response.status_code != 200:
                    print(f"❌ Failed to request task: {response.status_code}")
                    await asyncio.sleep(5)
                    return

                await response.aread()
                task_data = response.json()

            heartbeat = asyncio.create_task(self._heartbeat(base_url))

            print(f"\n📦 Received task: Frame {task_data['frame_number']}")

            # Download blend file if needed
            blend_path = await self._download_blend_file(
                base_url,
                task_data["job_id"],
                task_data["blend_file_hash"],
            )

            if not blend_path:
                print("❌ Failed to download blend file")
                await self._report_task_failure(task_data, "Failed to download blend file", base_url=base_url)
                return

            with tempfile.TemporaryDirectory(prefix="frame-", dir=self.temp_dir) as directory:
                self.bandwidth_manager.set_rendering_state(True)
                try:
                    result = await asyncio.to_thread(
                        self.blender.render_frame,
                        blend_path, task_data["frame_number"], Path(directory),
                        output_format=task_data.get("output_format", "PNG"),
                        render_settings=task_data.get("render_settings"),
                    )
                finally:
                    self.bandwidth_manager.set_rendering_state(False)

                if result.success:
                    print(f"✅ Rendered frame {result.frame} in {result.render_time:.1f}s")
                    await self._upload_result(base_url, task_data, result.output_file, result.render_time)
                else:
                    print(f"❌ Render failed: {result.error_message}")
                    await self._report_task_failure(task_data, result.error_message or "Render failed", base_url=base_url)

        except TaskReportTimeout as exc:
            print(f"❌ {exc}; releasing heartbeat so the assignment can expire")
        except httpx.RequestError as exc:
            print(f"❌ Coordinator request failed: {exc}")
        except Exception as e:
            print(f"❌ Error rendering task: {e}")
            if isinstance(task_data, dict) and "id" in task_data and "frame_number" in task_data:
                await self._report_task_failure(task_data, str(e), base_url=base_url)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    async def _download_blend_file(
        self,
        base_url: str,
        job_id: str,
        file_hash: str,
    ) -> Optional[Path]:
        """
        Download blend file from coordinator with optimization.

        Args:
            base_url: Coordinator base URL
            job_id: Job ID
            file_hash: Hash of the blend file

        Returns:
            Path to downloaded blend file, or None if failed
        """
        try:
            new_file_hasher(file_hash)  # Validate before using the digest as a filename.
            file_hash = file_hash.lower()
            blend_dir = self.temp_dir / "blend_files"
            blend_dir.mkdir(exist_ok=True)
            local_path = blend_dir / f"{file_hash}.blend"
            if local_path.exists():
                if await asyncio.to_thread(matches_file_hash, local_path, file_hash):
                    return local_path
                local_path.unlink()

            headers = {"X-API-Key": self.api_key}
            url = f"{base_url}/api/jobs/{job_id}/blend"
            chunked = ChunkedTransfer(url, local_path)
            file_size = await chunked.get_file_size(self.http_client, headers)
            if file_size > CHUNKED_THRESHOLD:
                try:
                    if await asyncio.wait_for(chunked.download(
                        self.http_client, headers, expected_hash=file_hash, total_size=file_size
                    ), timeout=DOWNLOAD_TIMEOUT):
                        return local_path
                except asyncio.TimeoutError:
                    pass
                print("⚠️ Ranged download failed; trying one full download")

            await asyncio.wait_for(self.transfer_optimizer.download_file(
                self.http_client, url, local_path, priority=TransferPriority.HIGH,
                headers=headers, expected_hash=file_hash, expected_size=file_size or None,
            ), timeout=DOWNLOAD_TIMEOUT)
            return local_path
        except (OSError, ValueError, httpx.HTTPError, asyncio.TimeoutError) as e:
            print(f"❌ Error downloading blend file: {e}")
            return None

    async def _upload_result(
        self,
        base_url: str,
        task_data: dict,
        output_file: Path,
        render_time: float,
    ) -> None:
        """
        Upload rendered frame to coordinator.

        Args:
            base_url: Coordinator base URL
            task_data: Task data
            output_file: Path to rendered output
            render_time: Time taken to render
        """
        async def send():
            with output_file.open("rb") as f:
                return await self.http_client.post(
                    f"{base_url}/api/tasks/{task_data['id']}/complete",
                    data={"render_time": str(render_time),
                          "assignment": task_data.get("assignment", "")},
                    files={"output": (output_file.name, f,
                                      mimetypes.guess_type(output_file.name)[0] or "application/octet-stream")},
                    headers={"X-API-Key": self.api_key},
                )

        try:
            if output_file is None:
                raise FileNotFoundError("Blender returned no output file")
            response = await self._retry_task_report(send, task_data, "upload")
        except OSError as exc:
            await self._report_task_failure(task_data, f"Cannot read output: {exc!r}", base_url=base_url)
            return
        if response.is_success:
            print(f"📤 Uploaded result for frame {task_data['frame_number']}")
        elif response.status_code not in {404, 409}:
            await self._report_task_failure(task_data, f"Upload refused: HTTP {response.status_code}", base_url=base_url)

    async def _retry_task_report(self, send, task_data: dict, operation: str) -> httpx.Response:
        """Keep the rendered result while the coordinator is temporarily unreachable."""
        try:
            return await asyncio.wait_for(
                self._retry_report_until_cancelled(send, task_data, operation),
                timeout=TASK_REPORT_TIMEOUT,
            )
        except asyncio.TimeoutError as exc:
            raise TaskReportTimeout(f"Frame {task_data['frame_number']} {operation} timed out") from exc

    async def _retry_report_until_cancelled(self, send, task_data, operation):
        delay = 1
        while True:
            try:
                response = await send()
                if response.status_code not in {408, 425, 429} and response.status_code < 500:
                    return response
                reason = f"HTTP {response.status_code}"
            except (httpx.RequestError, httpx.StreamError) as exc:
                reason = repr(exc)
            print(f"⚠️ Frame {task_data['frame_number']} {operation} failed: {reason}; retry in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _report_task_failure(self, task_data: dict, error: str, *, base_url: Optional[str] = None) -> None:
        """
        Report task failure to coordinator.

        Args:
            task_data: Task data
            error: Error message
        """
        if base_url is None:
            if not self.coordinator:
                return
            base_url = self.coordinator.base_url

        async def send():
            return await self.http_client.post(
                f"{base_url}/api/tasks/{task_data['id']}/fail",
                json={"error": error, "assignment": task_data.get("assignment")},
                headers={"X-API-Key": self.api_key},
            )
        try:
            response = await self._retry_task_report(send, task_data, "failure report")
        except TaskReportTimeout as exc:
            print(f"❌ {exc}; releasing heartbeat so the assignment can expire")
            return
        if not response.is_success:
            print(f"❌ Failure report for frame {task_data['frame_number']}: HTTP {response.status_code}")

    async def _sync_with_coordinator(self) -> None:
        """Synchronize settings and add-ons with coordinator."""
        if not self.coordinator:
            return

        print("\n🔄 Syncing with coordinator...")
        coordinator = self.coordinator
        base_url = coordinator.base_url

        try:
            response = await self.http_client.get(f"{base_url}/api/sync/manifest", headers={"X-API-Key": self.api_key})
            if response.status_code != 200:
                print(f"⚠️  Failed to get sync manifest: {response.status_code}")
                # We can still proceed, maybe just retry later
                await asyncio.sleep(5)
                return

            manifest = response.json()
            
            # 2. Sync Settings
            # In a real app we'd compare hashes. For now, always fetch to be safe/simple.
            await self._sync_settings(base_url)

            # 3. Sync Add-ons
            await self._sync_addons(base_url, manifest.get("addons_hash"))

            if self.coordinator is coordinator:
                self.is_synced = True
                print("✅ Synchronization complete")

        except Exception as e:
            print(f"❌ Sync failed: {e}")
            await asyncio.sleep(5)

    async def _sync_settings(self, base_url: str) -> None:
        """Download and apply global settings."""
        try:
            response = await self.http_client.get(f"{base_url}/api/sync/settings", headers={"X-API-Key": self.api_key})
            if response.status_code == 200:
                self.settings = response.json()
                print("📥 Global settings applied")
                # Here we would actually apply them to Blender or worker config
            else:
                print(f"⚠️  Failed to fetch settings: {response.status_code}")
        except Exception as e:
            print(f"❌ Error syncing settings: {e}")

    async def _sync_addons(self, base_url: str, remote_hash: str) -> None:
        """Download and install missing add-ons."""
        try:
            # Get list of add-ons
            response = await self.http_client.get(f"{base_url}/api/sync/addons", headers={"X-API-Key": self.api_key})
            if response.status_code != 200:
                return

            addons_list = response.json()
            if not addons_list:
                return

            print(f"📦 Checking {len(addons_list)} add-ons...")

            for addon in addons_list:
                addon_name = addon["name"]
                
                # Check if we need to install/update
                # For this MVP, we'll check if the zip exists in our cache
                local_zip = self.addons_dir / addon["filename"]
                
                need_download = True
                if local_zip.exists():
                    # Check size for basic verification
                    if local_zip.stat().st_size == addon["size"]:
                        need_download = False

                if need_download:
                    print(f"⬇️  Downloading add-on: {addon_name}")
                    await self._download_and_install_addon(base_url, addon, local_zip)
                else:
                    print(f"✅ Add-on already cached: {addon_name}")
                    # Ensure it's enabled even if cached
                    await self._enable_addon_in_blender(addon_name)

        except Exception as e:
            print(f"❌ Error syncing add-ons: {e}")

    async def _download_and_install_addon(self, base_url: str, addon_info: dict, target_path: Path) -> None:
        """Download and install a specific add-on."""
        try:
            async with self.http_client.stream(
                "GET", 
                f"{base_url}/api/sync/addons/{addon_info['name']}/download",
                headers={"X-API-Key": self.api_key},
            ) as response:
                if response.status_code != 200:
                    print(f"❌ Failed to download {addon_info['name']}")
                    return

                with open(target_path, "wb") as f:
                    async for chunk in response.aiter_bytes(8192):
                        f.write(chunk)
            
            print(f"📦 Installing {addon_info['name']}...")
            
            # Extract to Blender addons folder (using our mock folder for safety in test)
            with zipfile.ZipFile(target_path, 'r') as zip_ref:
                zip_ref.extractall(self.addons_dir)
                
            # Enable
            await self._enable_addon_in_blender(addon_info["name"])

        except Exception as e:
            print(f"❌ Failed to install {addon_info['name']}: {e}")

    async def _enable_addon_in_blender(self, addon_name: str) -> None:
        """Enable the add-on in Blender using CLI."""
        # For MVP we just log it since we might not have a full blender env in this dev container
        print(f"🔧 [Mock] Enabling add-on in Blender: {addon_name}")
        # Real impl: subprocess.run([self.specs.blender_path, "-b", "--python-expr", ...])
