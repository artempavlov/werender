"""Coordinator server for WeRender."""

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import math
import shutil
import sqlite3
import struct
import time
import zlib
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Depends, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ValidationError

from werender.core.blender import BlenderRenderer
from werender.core.job import (
    MAX_FRAME_ATTEMPTS, FrameTask, JobStatus, RenderJob, TaskStatus, validate_frame_range,
)
from werender.network.discovery import DiscoveryService, NodeInfo
from werender.network.sync import SyncManager
from werender.network.auth import AuthManager, get_api_key_dependency
from werender.network.security import BlendFileValidator, RateLimiter
from werender.scheduler import TaskScheduler, SchedulingStrategy, ChunkingStrategy, JobRequirements
from werender.core.job_templates import JobTemplateManager, JobExporter, JobPreset
from werender.core.resource_manager import ResourceManager
from werender.core.database import Database

ACTIVE_TASKS = {TaskStatus.ASSIGNED, TaskStatus.RENDERING}
TERMINAL_JOBS = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
WORKER_TIMEOUT = 30



class WorkerInfo(BaseModel):
    """Information about a connected worker."""

    worker_id: str
    worker_name: str
    cpu_cores: int
    gpu_name: str
    last_seen: float
    current_task_id: Optional[str] = None
    blender_version: str = ""
    ram_gb: Optional[float] = None
    tags: set[str] = set()


class UpdateJobRequest(BaseModel):
    """Request to update job properties."""

    start_frame: Optional[int] = None
    end_frame: Optional[int] = None


class CoordinatorServer:
    """Coordinator server that manages jobs and distributes tasks."""

    def __init__(self, port: int = 8420):
        """
        Initialize the coordinator server.

        Args:
            port: Port to run the server on
        """
        import socket
        import uuid

        self.port = port
        self.node_id = str(uuid.uuid4())[:8]

        # Get system specs
        from werender.utils.system import get_system_specs
        self.specs = get_system_specs()

        # Get Blender version
        try:
            self.blender_version = BlenderRenderer().get_version()
        except Exception:
            self.blender_version = ""

        # Storage
        self.jobs: dict[str, RenderJob] = {}
        self.workers: dict[str, WorkerInfo] = {}
        self.blender_path: Optional[Path] = None

        self._background_tasks: list[asyncio.Task] = []
        self._preparing_jobs: set[str] = set()
        self._pack_lock = asyncio.Lock()

        # Discovery service
        self.discovery = DiscoveryService(
            node_type="coordinator",
            port=self.port,
            node_id=self.node_id,
            properties={
                "hostname": socket.gethostname(),
                "cpu_cores": self.specs.cpu_cores,
                "blender_version": self.blender_version,
            },
        )

        # Sync Manager with asset cache
        self.config_dir = Path.home() / ".werender"
        self.config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.sync_manager = SyncManager(
            config_path=self.config_dir / "werender.json",
            addons_dir=self.config_dir / "addons",
            cache_db_path=self.config_dir / "asset_cache.db",
        )

        # Authentication Manager
        self.auth_manager = AuthManager(self.config_dir)
        
        # Security components
        self.rate_limiter = RateLimiter(max_requests=10, window_seconds=60)

        # Task Scheduler
        self.scheduler = TaskScheduler(
            strategy=SchedulingStrategy.HYBRID,
            chunking=ChunkingStrategy.SINGLE_FRAME,
        )

        # Job Template Manager
        self.template_manager = JobTemplateManager(
            templates_dir=self.config_dir / "templates"
        )

        # Resource Manager
        self.resource_manager = ResourceManager(
            check_disk_path=self.config_dir
        )

        # Database for persistence
        self.database = Database(self.config_dir / "werender.db")
        
        # Load persisted jobs and workers on startup
        self._load_persisted_data()

        # Setup FastAPI app
        self.app = FastAPI(title="WeRender Coordinator", lifespan=self._lifespan)
        self._setup_routes()

    @asynccontextmanager
    async def _lifespan(self, app):
        await self._on_startup()
        try:
            yield
        finally:
            await self._on_shutdown()

    def _setup_routes(self) -> None:
        """Setup FastAPI routes."""

        # Create authentication dependencies
        worker_auth = get_api_key_dependency(self.auth_manager, "worker")
        download_auth = get_api_key_dependency(self.auth_manager, ("worker", "coordinator"))
        coordinator_auth = get_api_key_dependency(self.auth_manager, "coordinator")

        # Worker endpoints (require worker authentication)
        self.app.get("/api/tasks/request", dependencies=[Depends(worker_auth)])(self._api_request_task)
        self.app.post("/api/tasks/{task_id}/complete", dependencies=[Depends(worker_auth)])(self._api_complete_task)
        self.app.post("/api/tasks/{task_id}/fail", dependencies=[Depends(worker_auth)])(self._api_fail_task)
        self.app.post("/api/heartbeat", dependencies=[Depends(worker_auth)])(self._api_heartbeat)
        self.app.get("/api/jobs/{job_id}/blend", dependencies=[Depends(download_auth)])(self._api_get_blend_file)
        self.app.head("/api/jobs/{job_id}/blend", dependencies=[Depends(download_auth)])(self._api_get_blend_file)

        # Job management endpoints (require coordinator authentication - CRITICAL SECURITY FIX)
        self.app.post("/api/jobs/create", dependencies=[Depends(coordinator_auth)])(self._api_create_job)
        self.app.get("/api/jobs", dependencies=[Depends(coordinator_auth)])(self._api_list_jobs)
        self.app.get("/api/jobs/{job_id}", dependencies=[Depends(coordinator_auth)])(self._api_get_job)
        self.app.patch("/api/jobs/{job_id}", dependencies=[Depends(coordinator_auth)])(self._api_update_job)
        self.app.post("/api/jobs/{job_id}/start", dependencies=[Depends(coordinator_auth)])(self._api_start_job)
        self.app.post("/api/jobs/{job_id}/pause", dependencies=[Depends(coordinator_auth)])(self._api_pause_job)
        self.app.post("/api/jobs/{job_id}/resume", dependencies=[Depends(coordinator_auth)])(self._api_resume_job)
        self.app.post("/api/jobs/{job_id}/cancel", dependencies=[Depends(coordinator_auth)])(self._api_cancel_job)
        self.app.post("/api/jobs/{job_id}/reload_blend", dependencies=[Depends(coordinator_auth)])(self._api_reload_blend)
        
        # Global job control endpoints
        self.app.post("/api/jobs/pause_all", dependencies=[Depends(coordinator_auth)])(self._api_pause_all_jobs)

        # Worker management endpoints (require coordinator authentication)
        self.app.get("/api/workers", dependencies=[Depends(coordinator_auth)])(self._api_list_workers)

        # Job template endpoints
        self.app.get("/api/templates", dependencies=[Depends(coordinator_auth)])(self._api_list_templates)
        self.app.get("/api/templates/{template_id}", dependencies=[Depends(coordinator_auth)])(self._api_get_template)
        self.app.post("/api/templates", dependencies=[Depends(coordinator_auth)])(self._api_create_template)
        self.app.delete("/api/templates/{template_id}", dependencies=[Depends(coordinator_auth)])(self._api_delete_template)
        self.app.post("/api/templates/preset/{preset}", dependencies=[Depends(coordinator_auth)])(self._api_create_preset_template)

        # Job export/import endpoints
        self.app.get("/api/jobs/{job_id}/export", dependencies=[Depends(coordinator_auth)])(self._api_export_job)
        self.app.post("/api/jobs/import", dependencies=[Depends(coordinator_auth)])(self._api_import_job)

        # Resource management endpoints
        self.app.get("/api/resources/disk", dependencies=[Depends(coordinator_auth)])(self._api_get_disk_space)
        self.app.get("/api/resources/workers/{worker_id}/limits", dependencies=[Depends(coordinator_auth)])(self._api_get_worker_limits)
        self.app.post("/api/resources/workers/{worker_id}/limits", dependencies=[Depends(coordinator_auth)])(self._api_set_worker_limits)
        self.app.post("/api/resources/workers/{worker_id}/reserve", dependencies=[Depends(coordinator_auth)])(self._api_reserve_worker)

        # Sync endpoints (require coordinator authentication - SECURITY FIX)
        self.app.get("/api/sync/manifest", dependencies=[Depends(worker_auth)])(self._api_get_sync_manifest)
        self.app.get("/api/sync/settings", dependencies=[Depends(worker_auth)])(self._api_get_settings)
        self.app.get("/api/sync/addons", dependencies=[Depends(download_auth)])(self._api_list_addons)
        self.app.get("/api/sync/addons/{addon_name}/download", dependencies=[Depends(download_auth)])(self._api_download_addon)

        # Serve static files (dashboard)
        dashboard_dir = Path(__file__).parent.parent / "dashboard"
        if dashboard_dir.exists():
            self.app.mount("/", StaticFiles(directory=str(dashboard_dir), html=True), name="dashboard")

    async def _on_startup(self) -> None:
        """Handle startup event - initialize services and background tasks."""
        # Start mDNS discovery to find workers on the network
        if not getattr(self, "direct_only", False):
            self.discovery.on_discovery = self._on_worker_discovered
            self.discovery.on_removal = self._on_worker_removed
            self.discovery.start()
        self._background_tasks = [asyncio.create_task(self._health_check_loop())]

    async def _on_shutdown(self) -> None:
        """Handle shutdown event."""
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if not getattr(self, "direct_only", False):
            self.discovery.stop()

    def _load_persisted_data(self) -> None:
        """Restore existing schema rows, reconstructing frames omitted by older releases."""
        self.database.mark_workers_offline()
        for row in self.database.list_jobs():
            metadata = row.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}
            options = {k: metadata[k] for k in (
                "requires_gpu", "min_memory_gb", "worker_tags", "depends_on", "scheduled_time",
            ) if k in metadata}
            try:
                job = RenderJob(
                    **options, id=row["id"], name=row["name"],
                    blend_file=row["blend_file_path"] or row["packed_file_path"] or Path("missing.blend"),
                    packed_file=row["packed_file_path"], blend_file_hash=row["blend_file_hash"],
                    frame_start=row["frame_start"], frame_end=row["frame_end"],
                    status=row["status"], priority=row["priority"] or 5,
                    created_at=row["created_at"], started_at=row["started_at"],
                    completed_at=row["completed_at"], blender_version=row["blender_version"] or "",
                    output_directory=row["output_dir"] or self._legacy_output_directory(row["id"]),
                    database_metadata=metadata,
                )
                by_frame = {}
                for data in self.database.get_job_tasks(job.id):
                    task = FrameTask(**data, output_file=data["output_file_path"])
                    if task.status in ACTIVE_TASKS:
                        task.reset()
                    elif task.status == TaskStatus.COMPLETED and task.output_file is None:
                        candidate = job.output_directory / f"{task.frame_number:04d}.png"
                        if candidate.is_file():
                            task.output_file = candidate
                    by_frame[task.frame_number] = task
                job.tasks = [by_frame.get(frame) or FrameTask(frame_number=frame)
                             for frame in range(job.frame_start, job.frame_end + 1)]
                if job.status == JobStatus.RUNNING:
                    job.check_completion()
                # Existing completed results retain their original paths. Future frames
                # for active legacy jobs no longer write into the old shared directory.
                if job.status not in TERMINAL_JOBS and not metadata.get("managed_output"):
                    job.output_directory = self._job_dir(job.id) / "frames"
                if job.status not in TERMINAL_JOBS and job.packed_file and not job.packed_file.is_file():
                    job.blend_file_hash = None
                if job.status not in TERMINAL_JOBS or row["status"] != job.status:
                    self._save_job_to_db(job, replace_tasks=True)
                self.jobs[job.id] = job
            except (ValidationError, ValueError, TypeError) as exc:
                print(f"⚠️  Failed to load job {row['id']}: {exc}")

    def _job_dir(self, job_id: str) -> Path:
        return self.config_dir / "jobs" / job_id

    @staticmethod
    def _legacy_output_directory(job_id: str) -> Path:
        directory = Path("output") / job_id
        return directory if directory.is_dir() else Path("output")

    def _save_job_to_db(self, job: RenderJob, tasks=(), *, replace_tasks=False) -> None:
        data = job.model_dump(mode="json", exclude={"tasks"})
        metadata = job.database_metadata | {k: data[k] for k in (
            "requires_gpu", "min_memory_gb", "worker_tags", "depends_on", "scheduled_time",
        )}
        metadata["managed_output"] = job.output_directory.is_relative_to(self._job_dir(job.id))
        data.update(blend_file_path=data["blend_file"], packed_file_path=data["packed_file"],
                    output_dir=data["output_directory"], metadata=metadata)
        if replace_tasks:
            tasks = job.tasks
        self.database.save_job_state(data, (
            dict(t.model_dump(mode="json"), job_id=job.id,
                 output_file_path=str(t.output_file) if t.output_file else None)
            for t in tasks
        ), replace_tasks=replace_tasks)

    def _change_job(self, job: RenderJob, *, tasks=None, replace_tasks=False, **changes) -> None:
        """Persist a staged state before exposing it to request handlers."""
        updated = job.model_copy(update=changes)
        if replace_tasks:
            updated.tasks = tasks
        self._save_job_to_db(updated, tasks or (), replace_tasks=replace_tasks)
        for key, value in changes.items():
            setattr(job, key, value)
        if replace_tasks:
            job.tasks = tasks

    def _commit_task(self, job: RenderJob, task: FrameTask, updated: FrameTask) -> None:
        status = job.status
        completed_at = job.completed_at
        if updated.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            other = [t.status for t in job.tasks if t is not task]
            if all(s in (TaskStatus.COMPLETED, TaskStatus.FAILED) for s in other):
                status = (JobStatus.FAILED if TaskStatus.FAILED in [updated.status, *other]
                          else JobStatus.COMPLETED)
                completed_at = datetime.now()
        self._change_job(job, tasks=[updated], status=status, completed_at=completed_at)
        for name in FrameTask.model_fields:
            setattr(task, name, getattr(updated, name))

    def _save_worker_to_db(self, worker_info: WorkerInfo) -> None:
        """Save a single worker to database."""
        try:
            worker_data = {
                "id": worker_info.worker_id,
                "name": worker_info.worker_name,
                "cpu_cores": worker_info.cpu_cores,
                "gpu_name": worker_info.gpu_name,
                "blender_version": worker_info.blender_version,
                "ram_gb": worker_info.ram_gb,
                "is_online": True,
                "last_seen": datetime.fromtimestamp(worker_info.last_seen),
                "current_task_id": worker_info.current_task_id,
            }
            self.database.save_worker(worker_data)
        except Exception as e:
            print(f"⚠️  Error saving worker to database: {e}")

    async def _health_check_loop(self) -> None:
        """
        Monitor worker health and handle timeouts.
        
        Workers must send heartbeat requests every 30 seconds. If a worker
        doesn't communicate within this window, we assume it's dead and
        re-queue any assigned task.
        """
        while True:
            await asyncio.sleep(5)
            current_time = time.time()

            for worker_id, worker in list(self.workers.items()):
                if current_time - worker.last_seen > WORKER_TIMEOUT:
                    try:
                        self._expire_worker(worker_id)
                    except (sqlite3.Error, OSError) as exc:
                        print(f"⚠️  Could not expire worker {worker_id}: {exc}")

    def _expire_worker(self, worker_id: str) -> None:
        worker = self.workers[worker_id]
        if worker.current_task_id:
            job, task = self._find_task(worker.current_task_id)
            if task and task.status in ACTIVE_TASKS and task.assigned_worker == worker_id:
                self._record_failure(job, task, "Worker disconnected or timed out")
        del self.workers[worker_id]
        self.database.update_worker_online_status(worker_id, False)

    async def _on_worker_discovered(self, node: NodeInfo) -> None:
        """
        Called when a new worker is discovered via mDNS.
        
        Note: This doesn't mean the worker is authenticated yet. The worker
        must make an authenticated API request before it can receive tasks.
        """
        print(f"✅ Worker discovered: {node.hostname} ({node.address}:{node.port})")

    async def _on_worker_removed(self, node: NodeInfo) -> None:
        """
        Called when mDNS detects a worker is no longer advertising itself.
        
        This happens when a worker shuts down gracefully or disconnects from
        the network. Any task the worker was working on gets re-queued.
        """
        # Discovery is advisory: hostname collisions or transient mDNS loss must
        # not invalidate an authenticated worker's active lease.
        print(f"Worker announcement removed: {node.hostname}")

    # ========== API Endpoints ==========

    async def _api_heartbeat(self, worker_id: str) -> dict:
        if worker := self.workers.get(worker_id):
            worker.last_seen = time.time()
        return {}

    async def _api_request_task(
        self,
        worker_id: str,
        worker_name: str,
        cpu_cores: int,
        gpu_name: str,
        blender_version: str = "",
        ram_gb: Optional[float] = None,
    ) -> dict:
        """
        Worker requests a new task to render.
        
        This endpoint requires worker authentication. The worker provides its
        specs which we use for scheduling decisions in the future (currently
        just for display).
        
        Returns 204 (No Content) if no tasks are available.
        """
        current_time = time.time()
        existing = self.workers.get(worker_id)
        if existing and existing.current_task_id:
            # A lost assignment reply must not let polling renew the orphaned lease.
            if current_time - existing.last_seen <= WORKER_TIMEOUT:
                raise HTTPException(status_code=204)
            self._expire_worker(worker_id)

        # Register or update this worker's information
        # The worker_id should be unique per worker instance
        if worker_id not in self.workers:
            print(f"📝 New worker registered: {worker_name} (ID: {worker_id})")
        worker_info = WorkerInfo(
            worker_id=worker_id,
            worker_name=worker_name,
            cpu_cores=cpu_cores,
            gpu_name=gpu_name,
            last_seen=current_time,
            current_task_id=None,
            blender_version=blender_version,
            ram_gb=ram_gb,
            tags=self.resource_manager.get_worker_limits(worker_id).tags,
        )
        self.workers[worker_id] = worker_info
        self._save_worker_to_db(worker_info)

        # Sort jobs by priority (higher priority first)
        sorted_jobs = sorted(
            [j for j in self.jobs.values() if j.status == "running"],
            key=lambda j: j.priority,
            reverse=True,
        )

        # Count current tasks for this worker
        worker_task_count = 1 if worker_info.current_task_id else 0

        # Look for the next pending task across all running jobs (prioritized)
        for job in sorted_jobs:
            if job.id in self._preparing_jobs:
                continue
            task = job.get_next_task()
            if not task:
                continue

            # Check resource limits
            can_accept, reason = self.resource_manager.can_worker_accept_task(
                worker_id=worker_id,
                worker_info=worker_info,
                job=job,
                current_task_count=worker_task_count,
            )
            if not can_accept:
                continue  # Try next job

            # Create job requirements from job properties
            job_requirements = JobRequirements(
                job_id=job.id,
                priority=job.priority,
                requires_gpu=job.requires_gpu,
                min_memory_gb=job.min_memory_gb,
                required_blender_version=job.blender_version if job.blender_version else None,
                worker_tags=job.worker_tags,
            )

            # A recent eligible idle worker may receive preference; stale/pinned
            # workers must never veto other peers. The pull request is the handoff.
            eligible_workers = {
                wid: worker for wid, worker in self.workers.items()
                if current_time - worker.last_seen <= 5
                and self.resource_manager.can_worker_accept_task(
                    wid, worker, job, int(bool(worker.current_task_id))
                )[0]
            }
            selected_worker = self.scheduler.select_worker(
                task=task,
                job=job,
                workers=eligible_workers,
                job_requirements=job_requirements,
            )

            if selected_worker == worker_id:
                # Legacy jobs may not have a hash persisted yet. Prepare once,
                # outside the event loop, and let the next poll claim the frame.
                if not job.blend_file_hash:
                    self._preparing_jobs.add(job.id)
                    try:
                        try:
                            scene = await asyncio.to_thread(self._scene_file, job)
                            if scene is None:
                                continue
                            digest = await asyncio.to_thread(self._hash_scene, scene)
                        except OSError:
                            continue
                        self._change_job(job, blend_file_hash=digest,
                                         packed_file=job.packed_file if scene == job.packed_file else None)
                    finally:
                        self._preparing_jobs.discard(job.id)
                    if (job.status != JobStatus.RUNNING or worker_info.current_task_id
                            or self.workers.get(worker_id) is not worker_info
                            or task not in job.tasks or task.status != TaskStatus.PENDING):
                        continue
                updated = task.model_copy()
                updated.assign_to(worker_id)
                updated.start_rendering()
                self._commit_task(job, task, updated)
                self.workers[worker_id].current_task_id = task.id
                self._save_worker_to_db(worker_info)
                self.scheduler.record_assignment(worker_id)

                # Return task details so the worker knows what to render
                return {
                    "id": task.id,
                    "job_id": job.id,
                    "frame_number": task.frame_number,
                    "blend_file_hash": job.blend_file_hash,
                    "blender_version": job.blender_version,
                    "assignment": task.assigned_at.isoformat(),
                }

        # No tasks available for this worker - worker should try again later
        raise HTTPException(status_code=204)

    async def _api_complete_task(
        self,
        task_id: str,
        output: UploadFile = File(...),
        render_time: str = Form(...),
        assignment: str = Form(""),
    ) -> dict:
        """API: Upload completed task result."""
        job, task = self._find_task(task_id)
        if not task or not job:
            raise HTTPException(status_code=404, detail="Task not found")
        self._check_task_assignment(task, assignment, allow_completed=True)
        if task.status == TaskStatus.COMPLETED:
            return {"status": "success"}
        try:
            duration = float(render_time)
            if not math.isfinite(duration) or duration < 0:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid render time")

        output_path = job.output_directory / f"{task.frame_number:04d}.png"
        partial = output_path.with_suffix(f".{uuid4().hex}.part")
        try:
            try:
                await asyncio.to_thread(self._stage_png, output.file, partial)
            except (ValueError, zlib.error):
                raise HTTPException(status_code=422, detail="Result is not a valid PNG")
            self._check_task_assignment(task, assignment, allow_completed=True)
            if task.status == TaskStatus.COMPLETED:
                return {"status": "success"}
            # Task may have been removed by an edit while the upload was copied.
            if task not in job.tasks:
                raise HTTPException(status_code=409, detail="Task was replaced")
            partial.replace(output_path)
            updated = task.model_copy()
            updated.complete(output_path)
            updated.render_time_seconds = duration
            updated.error_message = None
            try:
                self._commit_task(job, task, updated)
            except Exception:
                output_path.unlink(missing_ok=True)
                raise
        finally:
            partial.unlink(missing_ok=True)
        if task.assigned_worker:
            self.scheduler.update_worker_stats(task.assigned_worker, duration)
        self._release_task_worker(task)
        return {"status": "success"}

    async def _api_fail_task(self, task_id: str, error: dict) -> dict:
        """API: Report task failure."""
        job, task = self._find_task(task_id)
        if not task or not job:
            raise HTTPException(status_code=404, detail="Task not found")
        assignment = error.get("assignment")
        if (assignment and assignment == task.last_failed_assignment
                and task.status in (TaskStatus.PENDING, TaskStatus.FAILED)):
            return {"status": "success"}
        self._check_task_assignment(task, assignment)
        self._record_failure(job, task, str(error.get("error", "Unknown error"))[:4096])
        return {"status": "success"}

    def _find_task(self, task_id):
        for job in self.jobs.values():
            for task in job.tasks:
                if task.id == task_id:
                    return job, task
        return None, None

    def _record_failure(self, job, task, message):
        previous = task.model_copy()
        updated = task.model_copy()
        updated.failures += 1
        updated.last_failed_assignment = task.assigned_at.isoformat() if task.assigned_at else None
        if updated.failures >= MAX_FRAME_ATTEMPTS:
            updated.fail(message)
        else:
            updated.reset()
            updated.error_message = message
        self._commit_task(job, task, updated)
        self._release_task_worker(previous)

    @staticmethod
    def _check_task_assignment(task, assignment: str | None, *, allow_completed=False) -> None:
        valid_status = task.status in ACTIVE_TASKS or (allow_completed and task.status == TaskStatus.COMPLETED)
        if not valid_status or not task.assigned_at or task.assigned_at.isoformat() != assignment:
            raise HTTPException(status_code=409, detail="Task assignment is no longer active")

    def _release_task_worker(self, task) -> None:
        worker = self.workers.get(task.assigned_worker)
        if worker and worker.current_task_id == task.id:
            worker.current_task_id = None
            self._save_worker_to_db(worker)

    async def _api_get_blend_file(self, job_id: str) -> FileResponse:
        """API: Download blend file for a job."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        # Use packed file if available, otherwise original
        blend_file = await asyncio.to_thread(self._scene_file, job)
        if blend_file is None:
            raise HTTPException(status_code=404, detail="Blend file not found")

        return FileResponse(
            blend_file,
            media_type="application/octet-stream",
            filename=blend_file.name,
        )

    async def _api_create_job(
        self,
        file: UploadFile = File(...),
        frame_start: int = Form(...),
        frame_end: int = Form(...),
        name: str = Form(""),
        template_id: Optional[str] = Form(None),
    ) -> dict:
        """API: Create a new render job."""
        try:
            job = RenderJob(name=name or file.filename or "Unnamed job", blend_file=Path("scene.blend"),
                            frame_start=frame_start, frame_end=frame_end)
            if isinstance(template_id, str) and template_id:
                template = self.template_manager.get_template(template_id)
                if not template:
                    raise HTTPException(status_code=404, detail="Template not found")
                template.apply_to_job(job)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        await self._prepare_new_job(job, file)
        return {"job_id": job.id, "status": "pending"}

    async def _prepare_new_job(self, job: RenderJob, upload: UploadFile) -> None:
        can_accept, reason = await asyncio.to_thread(self.resource_manager.check_disk_space)
        if not can_accept:
            raise HTTPException(status_code=503, detail=f"Cannot accept job: {reason}")
        scene_dir = self._job_dir(job.id) / uuid4().hex
        job.blend_file, job.packed_file, version, job.blend_file_hash = await self._prepare_scene(upload, scene_dir)
        job.blender_version = job.blender_version or version
        job.output_directory = scene_dir / "frames"
        job.create_tasks()
        self._save_job_to_db(job, replace_tasks=True)
        self.jobs[job.id] = job

    async def _prepare_scene(self, upload: UploadFile, directory: Path, *, require_pack=False):
        filename = upload.filename or "scene.blend"
        if not filename.lower().endswith(".blend"):
            raise HTTPException(status_code=400, detail="File must be a .blend file")
        source = directory / "source.blend"
        packed = directory / "packed.blend"

        def prepare():
            directory.mkdir(parents=True, exist_ok=False, mode=0o700)
            try:
                with source.open("xb") as target:
                    self._copy_bounded(upload.file, target, BlendFileValidator.MAX_FILE_SIZE)
                valid, reason = BlendFileValidator.validate_blend_file(source)
                if not valid:
                    raise ValueError(f"Invalid .blend file: {reason}")
                version = ""
                result = None
                try:
                    renderer = BlenderRenderer()
                    candidate = renderer.pack_resources(source, output_path=packed)
                    if candidate != packed or not packed.is_file():
                        raise RuntimeError("Blender did not create a packed scene")
                    result = packed
                    version = renderer.get_version()
                except Exception as exc:
                    if require_pack and result is None:
                        raise HTTPException(status_code=500, detail="Could not pack replacement scene") from exc
                    print(f"⚠️  Scene packing/version detection unavailable: {exc}")
                return source, result, version, self._hash_scene(result or source)
            except Exception:
                shutil.rmtree(directory)
                raise

        async with self._pack_lock:
            try:
                return await asyncio.to_thread(prepare)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def _api_list_jobs(self) -> list[dict]:
        """API: List all jobs."""
        return [
            {
                "id": job.id,
                "name": job.name,
                "status": job.status,
                "progress": job.progress_percent,
                "total_frames": job.total_frames,
                "completed_frames": job.completed_frames,
                "frame_start": job.frame_start,  # BUG FIX: Missing frame_start
                "frame_end": job.frame_end,      # BUG FIX: Missing frame_end
            }
            for job in self.jobs.values()
        ]

    async def _api_get_job(self, job_id: str) -> dict:
        """API: Get job details."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        result = {
            "id": job.id,
            "name": job.name,
            "status": job.status,
            "progress": job.progress_percent,
            "total_frames": job.total_frames,
            "completed_frames": job.completed_frames,
            "frame_start": job.frame_start,  # BUG FIX: Missing frame_start
            "frame_end": job.frame_end,      # BUG FIX: Missing frame_end
            "output_directory": str(job.output_directory),
            "tasks": [
                {
                    "id": t.id,
                    "frame_number": t.frame_number,
                    "status": t.status,
                    "assigned_worker": t.assigned_worker,
                    "failures": t.failures,
                    "error_message": t.error_message,
                    "output_file": str(t.output_file) if t.output_file else None,
                }
                for t in job.tasks
            ],
        }
        return result

    async def _api_start_job(self, job_id: str) -> dict:
        """API: Start a job."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status == "running":
            return {"status": "already_running"}

        if job.status == JobStatus.PAUSED:
            return await self._api_resume_job(job_id)
        if job.status != JobStatus.PENDING:
            raise HTTPException(status_code=409, detail="Only a pending job can start")
        self._check_job_can_run(job)
        self._change_job(job, status=JobStatus.RUNNING, started_at=datetime.now(), completed_at=None)
        return {"status": "running"}

    def _check_job_can_run(self, job):
        self._check_not_preparing(job.id)
        # Check job dependencies
        can_start, reason = self._check_job_dependencies(job)
        if not can_start:
            raise HTTPException(status_code=400, detail=f"Cannot start job: {reason}")

        # Check scheduled time
        if job.scheduled_time and job.scheduled_time > datetime.now(job.scheduled_time.tzinfo):
            raise HTTPException(
                status_code=400,
                detail=f"Job is scheduled for {job.scheduled_time.isoformat()}"
            )

        count = sum(j.status == JobStatus.RUNNING for j in self.jobs.values())
        can_accept, reason = self.resource_manager.can_coordinator_accept_job(count)
        if not can_accept:
            raise HTTPException(status_code=503, detail=reason)

    def _check_not_preparing(self, job_id):
        if job_id in self._preparing_jobs:
            raise HTTPException(status_code=409, detail="Job scene is being prepared")

    def _change_job_state(self, job, status):
        active = [task for task in job.tasks if task.status in ACTIVE_TASKS]
        replacements = []
        for task in active:
            updated = task.model_copy()
            updated.reset()
            replacements.append(updated)
        self._change_job(job, status=status, tasks=replacements,
                         completed_at=datetime.now() if status == JobStatus.CANCELLED else None)
        for task in active:
            self._release_task_worker(task)
            task.reset()

    async def _api_pause_job(self, job_id: str) -> dict:
        """API: Pause a job."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status not in (JobStatus.RUNNING, JobStatus.PAUSED):
            raise HTTPException(status_code=409, detail="Only running jobs can pause")
        self._change_job_state(job, JobStatus.PAUSED)
        return {"status": "paused"}

    async def _api_resume_job(self, job_id: str) -> dict:
        """API: Resume a job."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status != JobStatus.PAUSED:
            raise HTTPException(status_code=409, detail="Only paused jobs can resume")
        self._check_job_can_run(job)
        self._change_job(job, status=JobStatus.RUNNING, started_at=job.started_at or datetime.now())
        return {"status": "running"}

    async def _api_cancel_job(self, job_id: str) -> dict:
        """API: Cancel a job."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        self._check_not_preparing(job.id)
        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            raise HTTPException(status_code=409, detail="Job already finished")
        if job.status != JobStatus.CANCELLED:
            self._change_job_state(job, JobStatus.CANCELLED)
        return {"status": "cancelled"}

    async def _api_pause_all_jobs(self) -> dict:
        """API: Pause all running jobs."""
        paused_count = 0
        for job in self.jobs.values():
            if job.status == JobStatus.RUNNING:
                self._change_job_state(job, JobStatus.PAUSED)
                paused_count += 1
                print(f"⏸️  Job {job.name} paused")
        
        return {"status": "success", "paused_count": paused_count}

    async def _api_update_job(self, job_id: str, request: UpdateJobRequest) -> dict:
        """API: Update job properties (frame range)."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        self._check_not_preparing(job_id)

        # Check if job is in a state that allows updates
        if job.status in ["running"]:
            raise HTTPException(
                status_code=400,
                detail="Cannot update job while it is running. Pause the job first."
            )

        # Get new frame values (use existing values if not provided)
        new_start = request.start_frame if request.start_frame is not None else job.frame_start
        new_end = request.end_frame if request.end_frame is not None else job.frame_end

        # Validate frame range
        try:
            validate_frame_range(new_start, new_end)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if (new_start, new_end) != (job.frame_start, job.frame_end):
            previous = job.tasks
            completed = {t.frame_number: t for t in previous if t.status == TaskStatus.COMPLETED}
            tasks = [completed.get(n) or FrameTask(frame_number=n)
                     for n in range(new_start, new_end + 1)]
            status = JobStatus.PENDING if job.status in TERMINAL_JOBS else job.status
            ended = None
            if all(t.status == TaskStatus.COMPLETED for t in tasks):
                status, ended = JobStatus.COMPLETED, datetime.now()
            output_directory = job.output_directory
            if not output_directory.is_relative_to(self._job_dir(job.id)):
                output_directory = self._job_dir(job.id) / "frames"
            self._change_job(job, tasks=tasks, replace_tasks=True, frame_start=new_start,
                             frame_end=new_end, status=status, completed_at=ended,
                             output_directory=output_directory)
            for task in previous:
                if task.status in ACTIVE_TASKS:
                    self._release_task_worker(task)

        return {
            "id": job.id,
            "name": job.name,
            "status": job.status,
            "progress": job.progress_percent,
            "total_frames": job.total_frames,
            "completed_frames": job.completed_frames,
            "frame_start": job.frame_start,
            "frame_end": job.frame_end,
        }

    async def _api_reload_blend(
        self,
        job_id: str,
        blend: UploadFile = File(...),
    ) -> dict:
        """API: Re-upload and replace a job's blend file."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        self._check_not_preparing(job_id)
        if job.status == JobStatus.RUNNING:
            raise HTTPException(status_code=400, detail="Pause the job before replacing its scene")

        directory = self._job_dir(job.id) / uuid4().hex
        self._preparing_jobs.add(job.id)
        try:
            source, packed, version, digest = await self._prepare_scene(blend, directory, require_pack=True)
            previous = job.tasks
            tasks = [FrameTask(frame_number=n) for n in range(job.frame_start, job.frame_end + 1)]
            self._change_job(job, tasks=tasks, replace_tasks=True, blend_file=source,
                             packed_file=packed, blender_version=version, blend_file_hash=digest,
                             status=JobStatus.PENDING, output_directory=directory / "frames",
                             started_at=None, completed_at=None)
            for task in previous:
                if task.status in ACTIVE_TASKS:
                    self._release_task_worker(task)
        finally:
            self._preparing_jobs.discard(job.id)

        return {
            "status": "success",
            "message": "Blend file reloaded successfully",
            "job_id": job.id,
        }

    async def _api_list_workers(self) -> list[dict]:
        """API: List all connected workers."""
        result = []
        for w in self.workers.values():
            # Find the current frame if worker has a task
            current_frame = None
            if w.current_task_id:
                for job in self.jobs.values():
                    for task in job.tasks:
                        if task.id == w.current_task_id:
                            current_frame = task.frame_number
                            break
                    if current_frame is not None:
                        break
            
            # Get system stats if available (optional - workers can send this)
            worker_data = {
                "worker_id": w.worker_id,
                "worker_name": w.worker_name,
                "cpu_cores": w.cpu_cores,
                "gpu_name": w.gpu_name,
                "blender_version": w.blender_version,
                "current_task_id": w.current_task_id,
                "current_frame": current_frame,
                "last_seen": w.last_seen,
            }
            result.append(worker_data)
        return result



    async def _api_get_sync_manifest(self) -> dict:
        """API: Get synchronization manifest."""
        return self.sync_manager.get_manifest()

    async def _api_get_settings(self) -> dict:
        """API: Get global settings."""
        return self.sync_manager.get_settings()

    async def _api_list_addons(self) -> list[dict]:
        """API: List available add-ons."""
        return self.sync_manager.get_addons_list()

    async def _api_download_addon(self, addon_name: str) -> FileResponse:
        """API: Download an add-on zip."""
        addons = self.sync_manager.get_addons_list()
        target_addon = next((a for a in addons if a["name"] == addon_name), None)
        
        if not target_addon:
             raise HTTPException(status_code=404, detail="Add-on not found")
        
        addon_path = self.sync_manager.addons_dir / target_addon["filename"]
        if not addon_path.exists():
            raise HTTPException(status_code=404, detail="Add-on file missing")

        return FileResponse(
            addon_path,
            media_type="application/zip",
            filename=addon_path.name,
        )

    # ========== Template Management Endpoints ==========

    async def _api_list_templates(self) -> list[dict]:
        """API: List all job templates."""
        templates = self.template_manager.list_templates()
        return [t.model_dump() for t in templates]

    async def _api_get_template(self, template_id: str) -> dict:
        """API: Get a specific template."""
        template = self.template_manager.get_template(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        return template.model_dump()

    async def _api_create_template(self, template_data: dict) -> dict:
        """API: Create a new template."""
        from werender.core.job_templates import JobTemplate
        try:
            template = JobTemplate(**template_data)
            template.check_supported()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        self.template_manager.save_template(template)
        return template.model_dump()

    async def _api_delete_template(self, template_id: str) -> dict:
        """API: Delete a template."""
        success = self.template_manager.delete_template(template_id)
        if not success:
            raise HTTPException(status_code=404, detail="Template not found")
        return {"status": "deleted"}

    async def _api_create_preset_template(self, preset: str) -> dict:
        """API: Create template from preset."""
        try:
            preset_enum = JobPreset(preset)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid preset: {preset}")
        
        template = self.template_manager.create_from_preset(preset_enum)
        return template.model_dump()

    # ========== Job Export/Import Endpoints ==========

    async def _api_export_job(self, job_id: str, include_tasks: bool = False) -> dict:
        """API: Export job configuration."""
        job = self.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return JobExporter.export_job(job, include_tasks=include_tasks)

    async def _api_import_job(
        self,
        file: UploadFile = File(...),
        blend_file: Optional[UploadFile] = File(None),
    ) -> dict:
        """API: Import job from configuration and an uploaded scene."""
        if blend_file is None:
            raise HTTPException(status_code=400, detail="Import requires an uploaded .blend file")
        config_data = await file.read(1024 * 1024 + 1)
        if len(config_data) > 1024 * 1024:
            raise HTTPException(status_code=413, detail="Job configuration is too large")
        try:
            config = json.loads(config_data)
            if not isinstance(config, dict):
                raise ValueError("Expected a JSON object")
            job = JobExporter.create_job_from_config(config, Path("scene.blend"), create_tasks=False)
        except (ValueError, KeyError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=f"Invalid job configuration: {exc}") from exc
        await self._prepare_new_job(job, blend_file)
        return {"job_id": job.id, "status": "imported"}

    # ========== Resource Management Endpoints ==========

    async def _api_get_disk_space(self) -> dict:
        """API: Get disk space information."""
        return self.resource_manager.get_disk_space_info()

    async def _api_get_worker_limits(self, worker_id: str) -> dict:
        """API: Get worker resource limits."""
        if worker_id not in self.workers:
            raise HTTPException(status_code=404, detail="Worker not found")
        
        limits = self.resource_manager.get_worker_limits(worker_id)
        return {
            "max_concurrent_tasks": limits.max_concurrent_tasks,
            "max_memory_per_task_gb": limits.max_memory_per_task_gb,
            "cpu_utilization_cap": limits.cpu_utilization_cap,
            "gpu_utilization_cap": limits.gpu_utilization_cap,
            "reserved_for_jobs": list(limits.reserved_for_jobs),
            "tags": list(limits.tags),
        }

    async def _api_set_worker_limits(self, worker_id: str, limits_data: dict) -> dict:
        """API: Set worker resource limits."""
        if worker_id not in self.workers:
            raise HTTPException(status_code=404, detail="Worker not found")
        
        from werender.core.resource_manager import WorkerResourceLimits
        
        try:
            limits = WorkerResourceLimits(
                max_concurrent_tasks=limits_data.get("max_concurrent_tasks", 1),
                max_memory_per_task_gb=limits_data.get("max_memory_per_task_gb"),
                cpu_utilization_cap=limits_data.get("cpu_utilization_cap", 1.0),
                gpu_utilization_cap=limits_data.get("gpu_utilization_cap", 1.0),
                reserved_for_jobs=set(limits_data.get("reserved_for_jobs", [])),
                tags=set(limits_data.get("tags", [])),
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        
        self.resource_manager.set_worker_limits(worker_id, limits)
        self.workers[worker_id].tags = limits.tags
        return {"status": "updated"}

    async def _api_reserve_worker(self, worker_id: str, job_id: str = Query(...)) -> dict:
        """API: Reserve a worker for a specific job."""
        if worker_id not in self.workers:
            raise HTTPException(status_code=404, detail="Worker not found")
        if job_id not in self.jobs:
            raise HTTPException(status_code=404, detail="Job not found")
        
        self.resource_manager.reserve_worker_for_job(worker_id, job_id)
        return {"status": "reserved"}

    # ========== Job Dependency Checking ==========

    def _check_job_dependencies(self, job: RenderJob) -> tuple[bool, Optional[str]]:
        """
        Check if job dependencies are satisfied.

        Returns:
            Tuple of (can_start, reason_if_not)
        """
        if not job.depends_on:
            return True, None

        for dep_id in job.depends_on:
            dep_job = self.jobs.get(dep_id)
            if not dep_job:
                return False, f"Dependency job {dep_id} not found"
            
            if dep_job.status != JobStatus.COMPLETED:
                return False, f"Dependency job {dep_id} is not completed (status: {dep_job.status})"

        return True, None

    @staticmethod
    def _scene_file(job: RenderJob) -> Path | None:
        for path in (job.packed_file, job.blend_file):
            if path and path.is_file():
                return path
        return None

    def _get_blend_file_hash(self, job: RenderJob) -> str:
        path = self._scene_file(job)
        if not path:
            raise FileNotFoundError("Blend file missing")
        return self._hash_scene(path)

    @staticmethod
    def _hash_scene(path: Path) -> str:
        # Keep the legacy worker cache/transfer protocol compatible.
        digest = hashlib.md5(usedforsecurity=False)
        with path.open("rb") as reader:
            for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _copy_bounded(reader, writer, limit):
        while block := reader.read(1024 * 1024):
            limit -= len(block)
            if limit < 0:
                raise ValueError("Upload too large")
            writer.write(block)

    @classmethod
    def _stage_png(cls, reader, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x+b") as target:
            cls._copy_bounded(reader, target, 256 * 1024 * 1024)
            target.seek(0)
            cls._verify_png(target)

    @staticmethod
    def _verify_png(reader) -> None:
        """Validate Blender's non-interlaced grayscale, RGB and RGBA PNG results."""
        def read_exact(size):
            data = reader.read(size)
            if len(data) != size:
                raise ValueError("Truncated PNG")
            return data

        if read_exact(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError("Missing PNG signature")
        if read_exact(8) != b"\x00\x00\x00\rIHDR":
            raise ValueError("Missing PNG header")
        header = read_exact(13)
        if struct.unpack(">I", read_exact(4))[0] != zlib.crc32(b"IHDR" + header):
            raise ValueError("Bad PNG header checksum")
        width, height, depth, color, comp, filters, interlace = struct.unpack(">IIBBBBB", header)
        channels = {0: 1, 2: 3, 4: 2, 6: 4}.get(color)
        if not channels or depth not in (8, 16) or comp or filters or interlace or not width or not height:
            raise ValueError("Unsupported PNG header")
        stride = 1 + width * channels * (depth // 8)
        expected = height * stride
        if expected > 512 * 1024 * 1024:
            raise ValueError("Decoded PNG too large")
        decoder = zlib.decompressobj()
        decoded = 0
        saw_data = data_ended = False
        while True:
            size, kind = struct.unpack(">I4s", read_exact(8))
            if not kind.isalpha():
                raise ValueError("Invalid PNG chunk type")
            if kind == b"IDAT":
                if data_ended:
                    raise ValueError("Nonconsecutive PNG image data")
                saw_data = True
            elif kind not in (b"IEND", b"PLTE") and kind[:1].isupper():
                raise ValueError("Unexpected critical PNG chunk")
            else:
                data_ended = saw_data
            if kind == b"IEND" and size:
                raise ValueError("Invalid PNG end chunk")
            crc = zlib.crc32(kind)
            while size:
                block = read_exact(min(size, 64 * 1024))
                size -= len(block)
                crc = zlib.crc32(block, crc)
                if kind == b"IDAT":
                    while block:
                        raw = decoder.decompress(block, 64 * 1024)
                        if decoder.unused_data or any(value > 4 for value in raw[(-decoded) % stride::stride]):
                            raise ValueError("Invalid PNG scanline data")
                        decoded += len(raw)
                        if decoded > expected:
                            raise ValueError("Unexpected PNG pixel count")
                        block = decoder.unconsumed_tail
            if struct.unpack(">I", read_exact(4))[0] != crc:
                raise ValueError("Bad PNG chunk checksum")
            if kind == b"IEND":
                if not saw_data or not decoder.eof or decoded != expected or reader.read(1):
                    raise ValueError("Incomplete or oversized PNG")
                return

    def run(self) -> None:
        """Run the coordinator server."""
        print("🎬 WeRender - Coordinator Mode")
        print("=" * 50)
        print(f"🖥️  Hostname: {self.specs.hostname}")
        print(f"🆔 Node ID: {self.node_id}")
        print(f"🖥️  CPU: {self.specs.cpu_cores} cores")
        print(f"💾 RAM: {self.specs.ram_gb} GB")
        print(f"🎨 Blender: {self.blender_version or 'Not found'}")
        print()
        print(f"🌐 Server running on http://0.0.0.0:{self.port}")
        if not getattr(self, "direct_only", False):
            print("📡 Advertising via mDNS")
        print("🔒 Authentication enabled - workers require API key")
        print()

        # Print authentication setup instructions
        self.auth_manager.print_setup_instructions()

        uvicorn.run(self.app, host="0.0.0.0", port=self.port)
