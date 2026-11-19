"""Transfer optimization module for WeRender.

This module provides optimized file transfer capabilities including:
- Compression for blend files and rendered output
- Bandwidth management and throttling
- Network quality detection
- Priority-based transfer queuing
"""

import asyncio
import gzip
import shutil
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List
import httpx
from werender.network.security import new_file_hasher


class CompressionLevel(str, Enum):
    """Compression level options."""

    NONE = "none"
    """No compression."""

    FAST = "fast"
    """Fast compression (lower ratio, faster)."""

    BALANCED = "balanced"
    """Balanced compression."""

    HIGH = "high"
    """High compression (higher ratio, slower)."""


class TransferPriority(str, Enum):
    """Transfer priority levels."""

    LOW = "low"
    """Low priority (background transfers)."""

    NORMAL = "normal"
    """Normal priority."""

    HIGH = "high"
    """High priority (urgent transfers)."""

    URGENT = "urgent"
    """Urgent priority (blocks other transfers)."""


@dataclass
class TransferStats:
    """Statistics for a file transfer."""

    bytes_transferred: int = 0
    """Total bytes transferred."""

    bytes_total: int = 0
    """Total bytes to transfer."""

    transfer_rate: float = 0.0
    """Transfer rate in bytes per second."""

    elapsed_time: float = 0.0
    """Elapsed time in seconds."""

    compressed_size: Optional[int] = None
    """Compressed size if compression was used."""

    compression_ratio: Optional[float] = None
    """Compression ratio if compression was used."""


class BandwidthManager:
    """Manages bandwidth allocation and throttling."""

    def __init__(
        self,
        max_bandwidth_mbps: Optional[float] = None,
        throttle_during_render: bool = True,
    ):
        """
        Initialize bandwidth manager.

        Args:
            max_bandwidth_mbps: Maximum bandwidth in Mbps (None = unlimited)
            throttle_during_render: Whether to throttle during active rendering
        """
        self.max_bandwidth_mbps = max_bandwidth_mbps
        self.throttle_during_render = throttle_during_render
        self.is_rendering = False
        self.active_transfers: Dict[str, TransferPriority] = {}
        self._lock = asyncio.Lock()

    def set_rendering_state(self, is_rendering: bool) -> None:
        """Set whether rendering is currently active."""
        self.is_rendering = is_rendering

    async def register_transfer(self, transfer_id: str, priority: TransferPriority) -> None:
        """Register an active transfer."""
        async with self._lock:
            self.active_transfers[transfer_id] = priority

    async def unregister_transfer(self, transfer_id: str) -> None:
        """Unregister a completed transfer."""
        async with self._lock:
            self.active_transfers.pop(transfer_id, None)

    def calculate_chunk_size(self, file_size: int, priority: TransferPriority) -> int:
        """
        Calculate optimal chunk size based on file size and priority.

        Args:
            file_size: Size of file in bytes
            priority: Transfer priority

        Returns:
            Optimal chunk size in bytes
        """
        # Base chunk sizes by priority
        base_sizes = {
            TransferPriority.URGENT: 64 * 1024,  # 64 KB
            TransferPriority.HIGH: 128 * 1024,  # 128 KB
            TransferPriority.NORMAL: 256 * 1024,  # 256 KB
            TransferPriority.LOW: 512 * 1024,  # 512 KB
        }

        base_size = base_sizes.get(priority, base_sizes[TransferPriority.NORMAL])

        # Adjust based on file size
        if file_size < 10 * 1024 * 1024:  # < 10 MB
            return base_size
        elif file_size < 100 * 1024 * 1024:  # < 100 MB
            return base_size * 2
        elif file_size < 1024 * 1024 * 1024:  # < 1 GB
            return base_size * 4
        else:  # >= 1 GB
            return base_size * 8

    async def should_throttle(self, priority: TransferPriority) -> bool:
        """
        Check if transfer should be throttled.

        Args:
            priority: Transfer priority

        Returns:
            True if transfer should be throttled
        """
        if not self.throttle_during_render or not self.is_rendering:
            return False

        # Don't throttle urgent or high priority transfers
        if priority in (TransferPriority.URGENT, TransferPriority.HIGH):
            return False

        # Check if there are higher priority transfers
        async with self._lock:
            higher_priorities = [
                TransferPriority.URGENT,
                TransferPriority.HIGH,
            ]
            for transfer_priority in self.active_transfers.values():
                if transfer_priority in higher_priorities:
                    return True

        return True

    async def get_throttle_delay(self, priority: TransferPriority) -> float:
        """
        Get delay to apply for throttling.

        Args:
            priority: Transfer priority

        Returns:
            Delay in seconds
        """
        if not await self.should_throttle(priority):
            return 0.0

        # Throttle delays by priority
        delays = {
            TransferPriority.NORMAL: 0.1,  # 100ms
            TransferPriority.LOW: 0.5,  # 500ms
        }

        return delays.get(priority, 0.0)


class NetworkQualityDetector:
    """Detects network quality and adjusts transfer parameters."""

    def __init__(self):
        """Initialize network quality detector."""
        self.samples: List[float] = []
        self.max_samples = 10

    async def measure_transfer_rate(
        self, bytes_transferred: int, elapsed_time: float
    ) -> float:
        """
        Measure transfer rate and update quality metrics.

        Args:
            bytes_transferred: Bytes transferred
            elapsed_time: Time elapsed in seconds

        Returns:
            Transfer rate in bytes per second
        """
        if elapsed_time > 0:
            rate = bytes_transferred / elapsed_time
            self.samples.append(rate)
            if len(self.samples) > self.max_samples:
                self.samples.pop(0)
            return rate
        return 0.0

    def get_quality(self) -> str:
        """
        Get current network quality assessment.

        Returns:
            Quality level: "excellent", "good", "fair", "poor"
        """
        if not self.samples:
            return "unknown"

        avg_rate = sum(self.samples) / len(self.samples)
        # Convert to Mbps for assessment
        mbps = (avg_rate * 8) / (1024 * 1024)

        if mbps >= 100:
            return "excellent"
        elif mbps >= 50:
            return "good"
        elif mbps >= 10:
            return "fair"
        else:
            return "poor"

    def get_recommended_compression(self) -> CompressionLevel:
        """
        Get recommended compression level based on network quality.

        Returns:
            Recommended compression level
        """
        quality = self.get_quality()
        if quality == "poor":
            return CompressionLevel.HIGH
        elif quality == "fair":
            return CompressionLevel.BALANCED
        else:
            return CompressionLevel.FAST


class TransferOptimizer:
    """Optimized file transfer handler."""

    def __init__(
        self,
        bandwidth_manager: Optional[BandwidthManager] = None,
        quality_detector: Optional[NetworkQualityDetector] = None,
    ):
        """
        Initialize transfer optimizer.

        Args:
            bandwidth_manager: Optional bandwidth manager
            quality_detector: Optional network quality detector
        """
        self.bandwidth_manager = bandwidth_manager or BandwidthManager()
        self.quality_detector = quality_detector or NetworkQualityDetector()

    def compress_file(
        self, file_path: Path, compression: CompressionLevel, output_path: Optional[Path] = None
    ) -> tuple[Path, int, float]:
        """
        Compress a file.

        Args:
            file_path: Path to file to compress
            compression: Compression level
            output_path: Optional output path (defaults to file_path + .gz)

        Returns:
            Tuple of (compressed_path, compressed_size, compression_ratio)
        """
        if compression == CompressionLevel.NONE:
            return file_path, file_path.stat().st_size, 1.0

        if output_path is None:
            output_path = Path(str(file_path) + ".gz")

        original_size = file_path.stat().st_size

        # Compression levels map to gzip compression levels
        compress_levels = {
            CompressionLevel.FAST: 1,
            CompressionLevel.BALANCED: 6,
            CompressionLevel.HIGH: 9,
        }

        compress_level = compress_levels.get(compression, 6)

        with open(file_path, "rb") as f_in:
            with gzip.open(output_path, "wb", compresslevel=compress_level) as f_out:
                shutil.copyfileobj(f_in, f_out)

        compressed_size = output_path.stat().st_size
        compression_ratio = compressed_size / original_size if original_size > 0 else 1.0

        return output_path, compressed_size, compression_ratio

    def decompress_file(self, compressed_path: Path, output_path: Path) -> None:
        """
        Decompress a gzip file.

        Args:
            compressed_path: Path to compressed file
            output_path: Path to write decompressed file
        """
        with gzip.open(compressed_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    async def download_file(
        self,
        http_client: httpx.AsyncClient,
        url: str,
        output_path: Path,
        priority: TransferPriority = TransferPriority.NORMAL,
        compression: Optional[CompressionLevel] = None,
        headers: Optional[Dict[str, str]] = None,
        progress_callback: Optional[callable] = None,
        *,
        expected_hash: Optional[str] = None,
        expected_size: Optional[int] = None,
    ) -> TransferStats:
        """
        Download a file with optimization.

        Args:
            http_client: HTTP client to use
            url: URL to download from
            output_path: Path to save file
            priority: Transfer priority
            compression: Compression level (None = auto-detect from URL)
            headers: Optional HTTP headers
            progress_callback: Optional callback(TransferStats) for progress updates

        Returns:
            TransferStats with transfer information
        """
        digest = new_file_hasher(expected_hash) if expected_hash is not None else None
        transfer_id = f"download_{id(output_path)}"
        await self.bandwidth_manager.register_transfer(transfer_id, priority)
        staging = None

        try:
            # Determine compression
            # A hashed download represents exact file bytes, including any on-disk compression.
            is_compressed = digest is None and url.endswith(".gz")
            request_headers = httpx.Headers(headers)
            if digest is not None:
                request_headers["Accept-Encoding"] = "identity"

            # Calculate chunk size
            # First, get file size from HEAD request if possible
            file_size = expected_size or 0

            chunk_size = self.bandwidth_manager.calculate_chunk_size(file_size, priority)

            stats = TransferStats(bytes_total=file_size)
            start_time = time.time()

            # Download file
            async with http_client.stream("GET", url, headers=request_headers) as response:
                if response.status_code != 200:
                    raise ValueError(f"Expected complete download, got HTTP {response.status_code}")
                identity = response.headers.get("Content-Encoding", "identity").lower() == "identity"
                if digest is not None and not identity:
                    raise ValueError("Expected identity Content-Encoding for file verification")

                # Update total size from response if available
                declared = response.headers.get("Content-Length")
                size = int(declared) if identity and declared is not None else expected_size
                if expected_size is not None and size != expected_size:
                    raise ValueError("Download Content-Length disagrees with HEAD")
                if size is not None and size < 0:
                    raise ValueError("Invalid Content-Length")
                stats.bytes_total = size or 0

                output_path.parent.mkdir(parents=True, exist_ok=True)
                staging = tempfile.TemporaryDirectory(prefix=".download-", dir=output_path.parent)
                temp_path = Path(staging.name) / "payload"

                with open(temp_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size):
                        stats.bytes_transferred += len(chunk)
                        if size is not None and stats.bytes_transferred > size:
                            raise ValueError("Download is longer than declared")
                        f.write(chunk)
                        if digest is not None:
                            digest.update(chunk)

                        # Throttle if needed
                        if await self.bandwidth_manager.should_throttle(priority):
                            delay = await self.bandwidth_manager.get_throttle_delay(priority)
                            await asyncio.sleep(delay)

                        # Update stats
                        stats.elapsed_time = time.time() - start_time
                        if stats.elapsed_time > 0:
                            stats.transfer_rate = stats.bytes_transferred / stats.elapsed_time
                            await self.quality_detector.measure_transfer_rate(
                                len(chunk), stats.elapsed_time
                            )

                        # Call progress callback
                        if progress_callback:
                            progress_callback(stats)

                if size is not None and stats.bytes_transferred != size:
                    raise ValueError("Download is shorter than declared")
                if digest is not None and digest.hexdigest() != expected_hash.lower():
                    raise ValueError("Downloaded file digest does not match")

                if is_compressed:
                    decompressed_path = Path(staging.name) / "decompressed"
                    self.decompress_file(temp_path, decompressed_path)
                    decompressed_path.replace(output_path.with_suffix(""))
                else:
                    temp_path.replace(output_path)

            return stats

        finally:
            if staging is not None:
                staging.cleanup()
            await self.bandwidth_manager.unregister_transfer(transfer_id)

    async def upload_file(
        self,
        http_client: httpx.AsyncClient,
        url: str,
        file_path: Path,
        priority: TransferPriority = TransferPriority.NORMAL,
        compression: Optional[CompressionLevel] = None,
        headers: Optional[Dict[str, str]] = None,
        progress_callback: Optional[callable] = None,
    ) -> TransferStats:
        """
        Upload a file with optimization.

        Args:
            http_client: HTTP client to use
            url: URL to upload to
            file_path: Path to file to upload
            priority: Transfer priority
            compression: Compression level (None = auto-detect)
            headers: Optional HTTP headers
            progress_callback: Optional callback(TransferStats) for progress updates

        Returns:
            TransferStats with transfer information
        """
        transfer_id = f"upload_{id(file_path)}"
        await self.bandwidth_manager.register_transfer(transfer_id, priority)

        try:
            # Determine compression
            if compression is None:
                compression = self.quality_detector.get_recommended_compression()

            original_size = file_path.stat().st_size
            upload_path = file_path
            should_decompress = False

            # Compress if needed
            if compression != CompressionLevel.NONE:
                compressed_path, compressed_size, ratio = self.compress_file(
                    file_path, compression
                )
                upload_path = compressed_path
                should_decompress = True
                stats = TransferStats(
                    bytes_total=compressed_size,
                    compressed_size=compressed_size,
                    compression_ratio=ratio,
                )
            else:
                stats = TransferStats(bytes_total=original_size)

            # Set compression header if compressed
            upload_headers = headers.copy() if headers else {}
            if should_decompress:
                upload_headers["Content-Encoding"] = "gzip"

            start_time = time.time()

            # Upload file
            with open(upload_path, "rb") as f:
                # Read in chunks for progress tracking
                chunk_size = self.bandwidth_manager.calculate_chunk_size(
                    stats.bytes_total, priority
                )

                async def file_generator():
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break

                        stats.bytes_transferred += len(chunk)

                        # Throttle if needed
                        if await self.bandwidth_manager.should_throttle(priority):
                            delay = await self.bandwidth_manager.get_throttle_delay(priority)
                            await asyncio.sleep(delay)

                        # Update stats
                        stats.elapsed_time = time.time() - start_time
                        if stats.elapsed_time > 0:
                            stats.transfer_rate = stats.bytes_transferred / stats.elapsed_time
                            await self.quality_detector.measure_transfer_rate(
                                len(chunk), stats.elapsed_time
                            )

                        # Call progress callback
                        if progress_callback:
                            progress_callback(stats)

                        yield chunk

                response = await http_client.post(
                    url, content=file_generator(), headers=upload_headers
                )

            # Clean up compressed file if created
            if should_decompress and upload_path != file_path:
                upload_path.unlink()

            if response.status_code not in (200, 201):
                raise Exception(f"HTTP {response.status_code}")

            return stats

        finally:
            await self.bandwidth_manager.unregister_transfer(transfer_id)
