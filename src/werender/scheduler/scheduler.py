"""Intelligent scheduler for WeRender job distribution."""

from enum import Enum
import re
from typing import Optional, TYPE_CHECKING
from dataclasses import dataclass

from werender.core.job import RenderJob, FrameTask, TaskStatus

if TYPE_CHECKING:
    from werender.network.coordinator import WorkerInfo


class SchedulingStrategy(str, Enum):
    """Scheduling algorithm strategies."""

    ROUND_ROBIN = "round_robin"
    """Simple round-robin distribution."""

    PERFORMANCE_BASED = "performance_based"
    """Assign to fastest workers first."""

    FAIR_DISTRIBUTION = "fair_distribution"
    """Ensure all workers get equal work."""

    HYBRID = "hybrid"
    """Combine performance and fairness."""


class ChunkingStrategy(str, Enum):
    """Frame chunking strategies."""

    SINGLE_FRAME = "single_frame"
    """One frame per task (current default)."""

    MULTI_FRAME = "multi_frame"
    """Multiple frames per task (2-10 frames)."""

    ADAPTIVE = "adaptive"
    """Adjust chunk size based on render time."""


@dataclass
class WorkerCapabilities:
    """Worker capabilities and requirements."""

    worker_id: str
    cpu_cores: int
    gpu_name: str
    gpu_vram_gb: Optional[float] = None
    ram_gb: Optional[float] = None
    blender_version: str = ""
    current_task_id: Optional[str] = None
    tasks_completed: int = 0
    average_render_time: Optional[float] = None
    is_idle: bool = True
    tags: set[str] = None  # Worker groups/tags

    def __post_init__(self):
        """Initialize default values."""
        if self.tags is None:
            self.tags = set()

    def can_handle_job(self, job: RenderJob) -> tuple[bool, str]:
        """
        Check if worker can handle a job.

        Returns:
            Tuple of (can_handle, reason)
        """
        # Check Blender version compatibility
        if job.blender_version and self.blender_version:
            required = re.match(r"^(\d+\.\d+)\b", job.blender_version)
            actual = re.match(r"^(\d+\.\d+)\b", self.blender_version)
            compatible = (
                required.group(1) == actual.group(1)
                if required and actual else job.blender_version == self.blender_version
            )
            if not compatible:
                return False, f"Blender version mismatch: {job.blender_version} vs {self.blender_version}"

        # Check if worker is idle
        if not self.is_idle:
            return False, "Worker is busy"

        return True, "OK"


@dataclass
class JobRequirements:
    """Job requirements and preferences."""

    job_id: str
    priority: int = 5  # 1-10, higher is more urgent
    requires_gpu: bool = False
    min_memory_gb: Optional[float] = None
    required_blender_version: Optional[str] = None
    worker_tags: set[str] = None  # Required worker tags
    chunk_size: int = 1  # Frames per task
    max_chunk_size: int = 10

    def __post_init__(self):
        """Initialize default values."""
        if self.worker_tags is None:
            self.worker_tags = set()


class TaskScheduler:
    """Intelligent task scheduler with multiple strategies."""

    def __init__(
        self,
        strategy: SchedulingStrategy = SchedulingStrategy.HYBRID,
        chunking: ChunkingStrategy = ChunkingStrategy.SINGLE_FRAME,
    ):
        """
        Initialize the scheduler.

        Args:
            strategy: Scheduling algorithm to use
            chunking: Frame chunking strategy
        """
        self.strategy = strategy
        self.chunking = chunking
        self.worker_stats: dict[str, dict] = {}  # Track worker performance
        self._last_assignment: dict[str, int] = {}
        self._assignments = 0

    def record_assignment(self, worker_id: str) -> None:
        self._assignments += 1
        self._last_assignment[worker_id] = self._assignments

    def select_worker(
        self,
        task: FrameTask,
        job: RenderJob,
        workers: dict[str, "WorkerInfo"],
        job_requirements: Optional[JobRequirements] = None,
    ) -> Optional[str]:
        """
        Select the best worker for a task.

        Args:
            task: The task to assign
            job: The job this task belongs to
            workers: Available workers
            job_requirements: Job requirements and preferences

        Returns:
            Worker ID if a suitable worker is found, None otherwise
        """
        if not workers:
            return None

        # Filter workers by capabilities
        available_workers = []
        for worker_id, worker_info in workers.items():
            # Convert WorkerInfo to WorkerCapabilities
            capabilities = self._worker_info_to_capabilities(worker_id, worker_info)
            
            # Check if worker can handle the job
            can_handle, reason = capabilities.can_handle_job(job)
            if not can_handle:
                continue

            # Check job requirements if provided
            requirements = job_requirements or job
            gpu = capabilities.gpu_name.strip().casefold()
            if requirements.requires_gpu and gpu in {"", "none", "cpu", "n/a", "unknown"}:
                continue
            if requirements.min_memory_gb and (capabilities.ram_gb or 0) < requirements.min_memory_gb:
                continue
            if not requirements.worker_tags.issubset(capabilities.tags):
                continue

            available_workers.append((worker_id, capabilities))

        if not available_workers:
            return None

        # Apply scheduling strategy
        if self.strategy == SchedulingStrategy.ROUND_ROBIN:
            return self._round_robin_select(available_workers)
        elif self.strategy == SchedulingStrategy.PERFORMANCE_BASED:
            return self._performance_based_select(available_workers)
        elif self.strategy == SchedulingStrategy.FAIR_DISTRIBUTION:
            return self._fair_distribution_select(available_workers)
        elif self.strategy == SchedulingStrategy.HYBRID:
            return self._hybrid_select(available_workers)
        else:
            return self._round_robin_select(available_workers)

    def _worker_info_to_capabilities(self, worker_id: str, worker_info: "WorkerInfo") -> WorkerCapabilities:
        """Convert WorkerInfo to WorkerCapabilities."""
        stats = self.worker_stats.get(worker_id, {})
        
        return WorkerCapabilities(
            worker_id=worker_id,
            cpu_cores=worker_info.cpu_cores,
            gpu_name=worker_info.gpu_name,
            blender_version=getattr(worker_info, "blender_version", ""),
            ram_gb=getattr(worker_info, "ram_gb", None),
            tags=getattr(worker_info, "tags", set()),
            current_task_id=worker_info.current_task_id,
            is_idle=worker_info.current_task_id is None,
            tasks_completed=stats.get("tasks_completed", 0),
            average_render_time=stats.get("average_render_time"),
        )

    def _round_robin_select(self, workers: list[tuple[str, WorkerCapabilities]]) -> str:
        """Round-robin selection: cycle through workers."""
        return min(workers, key=lambda pair: self._assignment_order(pair[0]))[0]

    def _assignment_order(self, worker_id: str) -> tuple[int, str]:
        return self._last_assignment.get(worker_id, 0), worker_id

    def _performance_based_select(self, workers: list[tuple[str, WorkerCapabilities]]) -> str:
        """Performance-based: assign to fastest workers first."""
        # Sort by performance (faster = lower average render time)
        # Workers with no stats go to the end
        def sort_key(x: tuple[str, WorkerCapabilities]) -> tuple[bool, float]:
            worker_id, caps = x
            if caps.average_render_time is None:
                return (True, float('inf'))  # No stats = lowest priority
            return (False, caps.average_render_time)

        workers.sort(key=lambda pair: (*sort_key(pair), self._assignment_order(pair[0])))
        
        # Prefer idle workers
        for worker_id, capabilities in workers:
            if capabilities.is_idle:
                return worker_id
        
        return workers[0][0]

    def _fair_distribution_select(self, workers: list[tuple[str, WorkerCapabilities]]) -> str:
        """Fair distribution: ensure all workers get equal work."""
        # Sort by tasks completed (ascending)
        workers.sort(key=lambda pair: (pair[1].tasks_completed, self._assignment_order(pair[0])))
        
        # Prefer idle workers with least tasks
        for worker_id, capabilities in workers:
            if capabilities.is_idle:
                return worker_id
        
        return workers[0][0]

    def _hybrid_select(self, workers: list[tuple[str, WorkerCapabilities]]) -> str:
        """Hybrid: balance performance and fairness."""
        # Calculate score: performance score + fairness score
        def calculate_score(worker_id: str, caps: WorkerCapabilities) -> float:
            # Performance score (lower render time = higher score)
            perf_score = 0.0
            if caps.average_render_time:
                # Normalize: faster workers get higher score
                # Use inverse (1/time) but scale it
                perf_score = 100.0 / (caps.average_render_time + 1.0)
            else:
                perf_score = 50.0  # Default for workers without stats

            # Fairness score (fewer tasks = higher score)
            fairness_score = 100.0 / (caps.tasks_completed + 1.0)

            # Weighted combination (60% performance, 40% fairness)
            return 0.6 * perf_score + 0.4 * fairness_score

        # Sort by score (descending)
        workers.sort(key=lambda pair: (-calculate_score(*pair), self._assignment_order(pair[0])))
        
        # Prefer idle workers
        for worker_id, capabilities in workers:
            if capabilities.is_idle:
                return worker_id
        
        return workers[0][0]

    def create_chunks(
        self,
        job: RenderJob,
        chunk_size: Optional[int] = None,
    ) -> list[list[FrameTask]]:
        """
        Create frame chunks based on chunking strategy.

        Args:
            job: The job to chunk
            chunk_size: Override chunk size (if None, uses strategy default)

        Returns:
            List of task chunks (each chunk is a list of FrameTask)
        """
        if not job.tasks:
            return []

        if self.chunking == ChunkingStrategy.SINGLE_FRAME:
            # One frame per chunk
            return [[task] for task in job.tasks if task.status == TaskStatus.PENDING]

        elif self.chunking == ChunkingStrategy.MULTI_FRAME:
            # Multiple frames per chunk
            size = chunk_size or 5  # Default 5 frames per chunk
            pending_tasks = [t for t in job.tasks if t.status == TaskStatus.PENDING]
            chunks = []
            
            for i in range(0, len(pending_tasks), size):
                chunk = pending_tasks[i:i + size]
                chunks.append(chunk)
            
            return chunks

        elif self.chunking == ChunkingStrategy.ADAPTIVE:
            # Adaptive chunking based on render time
            if not job.average_render_time:
                # No data yet, use single frame
                return [[task] for task in job.tasks if task.status == TaskStatus.PENDING]
            
            # If frames render quickly (< 30s), use larger chunks
            # If frames render slowly (> 5min), use smaller chunks
            avg_time = job.average_render_time
            if avg_time < 30:
                size = 10  # Fast renders: 10 frames per chunk
            elif avg_time < 300:
                size = 5   # Medium renders: 5 frames per chunk
            else:
                size = 1   # Slow renders: 1 frame per chunk

            pending_tasks = [t for t in job.tasks if t.status == TaskStatus.PENDING]
            chunks = []
            
            for i in range(0, len(pending_tasks), size):
                chunk = pending_tasks[i:i + size]
                chunks.append(chunk)
            
            return chunks

        else:
            # Default: single frame
            return [[task] for task in job.tasks if task.status == TaskStatus.PENDING]

    def update_worker_stats(self, worker_id: str, render_time: float) -> None:
        """
        Update worker performance statistics.

        Args:
            worker_id: Worker identifier
            render_time: Time taken to render the frame
        """
        if worker_id not in self.worker_stats:
            self.worker_stats[worker_id] = {
                "tasks_completed": 0,
                "total_render_time": 0.0,
                "average_render_time": None,
            }

        stats = self.worker_stats[worker_id]
        stats["tasks_completed"] += 1
        stats["total_render_time"] += render_time
        stats["average_render_time"] = stats["total_render_time"] / stats["tasks_completed"]

    def get_worker_performance(self, worker_id: str) -> Optional[dict]:
        """
        Get worker performance statistics.

        Args:
            worker_id: Worker identifier

        Returns:
            Performance stats dict or None if worker has no stats
        """
        return self.worker_stats.get(worker_id)
