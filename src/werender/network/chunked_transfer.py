"""Parallel, verified HTTP range downloads."""

import asyncio
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import httpx

from werender.network.security import new_file_hasher


@dataclass
class TransferProgress:
    total_bytes: int
    downloaded_bytes: int
    chunks_total: int
    chunks_completed: int
    chunks_failed: int


class ChunkedTransfer:
    """Attempt a verified ranged download; callers can fall back to a full GET.

    An interrupted attempt never leaves reusable parts or a published file.
    """

    def __init__(
        self,
        url: str,
        output_path: Path,
        chunk_size: int = 10 * 1024 * 1024,
        max_parallel_chunks: int = 4,
    ):
        if chunk_size < 1 or max_parallel_chunks < 1:
            raise ValueError("Chunk size and parallelism must be positive")
        self.url = url
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.max_parallel_chunks = max_parallel_chunks

    async def get_file_size(self, http_client: httpx.AsyncClient, headers=None) -> int:
        try:
            request_headers = httpx.Headers(headers)
            request_headers["Accept-Encoding"] = "identity"
            response = await http_client.head(self.url, headers=request_headers)
            size = response.headers.get("Content-Length", "")
            if (
                response.status_code == 200
                and response.headers.get("Content-Encoding", "identity").lower() == "identity"
                and re.fullmatch(r"[0-9]+", size)
            ):
                return int(size)
        except (httpx.HTTPError, ValueError):
            pass
        return 0

    async def download(
        self,
        http_client: httpx.AsyncClient,
        headers=None,
        progress_callback: Optional[Callable[[TransferProgress], None]] = None,
        *,
        expected_hash: str,
        total_size: Optional[int] = None,
    ) -> bool:
        digest = new_file_hasher(expected_hash)
        if total_size is None:
            total_size = await self.get_file_size(http_client, headers)
        if total_size <= 0:
            return False

        count = (total_size + self.chunk_size - 1) // self.chunk_size
        progress = TransferProgress(total_size, 0, count, 0, 0)
        next_part = iter(range(count))
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=".ranges-", dir=self.output_path.parent) as directory:
            staging = Path(directory)

            async def fetch_parts():
                for index in next_part:
                    start = index * self.chunk_size
                    end = min(start + self.chunk_size, total_size) - 1
                    size = end - start + 1
                    request_headers = httpx.Headers(headers)
                    request_headers["Accept-Encoding"] = "identity"
                    request_headers["Range"] = f"bytes={start}-{end}"
                    async with http_client.stream("GET", self.url, headers=request_headers) as response:
                        content_range = response.headers.get("Content-Range", "").lower()
                        content_length = response.headers.get("Content-Length")
                        if (
                            response.status_code != 206
                            or content_range != f"bytes {start}-{end}/{total_size}"
                            or (content_length is not None and content_length != str(size))
                            or response.headers.get("Content-Encoding", "identity").lower() != "identity"
                        ):
                            raise ValueError("Invalid HTTP range response")
                        written = 0
                        with (staging / str(index)).open("wb") as target:
                            async for block in response.aiter_bytes(256 * 1024):
                                written += len(block)
                                if written > size:
                                    raise ValueError("Range response is too long")
                                target.write(block)
                        if written != size:
                            raise ValueError("Range response is incomplete")
                    progress.downloaded_bytes += written
                    progress.chunks_completed += 1
                    if progress_callback:
                        progress_callback(progress)

            tasks = [asyncio.create_task(fetch_parts()) for _ in range(min(count, self.max_parallel_chunks))]
            try:
                await asyncio.gather(*tasks)
            except (httpx.HTTPError, OSError, ValueError):
                return False
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            # Yield during assembly so active assignments can still send heartbeats.
            staged_file = staging / "assembled"
            with staged_file.open("wb") as target:
                for index in range(count):
                    with (staging / str(index)).open("rb") as part:
                        for block in iter(lambda: part.read(1024 * 1024), b""):
                            target.write(block)
                            digest.update(block)
                            await asyncio.sleep(0)
                    (staging / str(index)).unlink()
            if digest.hexdigest() != expected_hash.lower():
                return False
            staged_file.replace(self.output_path)
            return True
