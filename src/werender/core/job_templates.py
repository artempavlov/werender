"""Job templates and presets for WeRender."""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from werender.core.job import RenderJob


class JobPreset(str, Enum):
    """Built-in job presets."""

    ANIMATION = "animation"
    """Full animation render (high quality)."""

    STILL = "still"
    """Single frame render (production quality)."""

    TEST = "test"
    """Test render (low quality, fast)."""

    PREVIEW = "preview"
    """Preview render (medium quality)."""


class JobTemplate(BaseModel):
    """A reusable job template."""

    id: str = Field(default_factory=lambda: uuid4().hex, pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    """Unique template identifier."""

    name: str
    """Template name."""

    description: str = ""
    """Template description."""

    priority: int = Field(default=5, ge=1, le=10)
    """Default priority (1-10)."""

    requires_gpu: bool = False
    """Whether GPU is required."""

    min_memory_gb: Optional[float] = None
    """Minimum memory requirement."""

    worker_tags: Set[str] = Field(default_factory=set)
    """Required worker tags."""

    render_settings: Dict[str, Any] = Field(default_factory=dict)
    """Render settings overrides."""

    output_format: str = "PNG"
    """Output format (PNG, JPEG, EXR, etc.)."""

    created_at: datetime = Field(default_factory=datetime.now)
    """When template was created."""

    @classmethod
    def from_preset(cls, preset: JobPreset) -> "JobTemplate":
        """Create template from built-in preset."""
        presets = {
            JobPreset.ANIMATION: cls(
                name="Animation",
                description="High scheduling priority; scene render settings are unchanged",
                priority=7,
                requires_gpu=True,
                min_memory_gb=8.0,
            ),
            JobPreset.STILL: cls(
                name="Still",
                description="Very high scheduling priority; scene render settings are unchanged",
                priority=8,
                requires_gpu=True,
                min_memory_gb=16.0,
            ),
            JobPreset.TEST: cls(
                name="Test Render",
                description="Low scheduling priority; scene render settings are unchanged",
                priority=3,
                requires_gpu=False,
                min_memory_gb=2.0,
                output_format="PNG",
            ),
            JobPreset.PREVIEW: cls(
                name="Preview",
                description="Normal scheduling priority; scene render settings are unchanged",
                priority=5,
                requires_gpu=False,
                min_memory_gb=4.0,
                output_format="PNG",
            ),
        }
        return presets[preset]

    def check_supported(self) -> None:
        if self.render_settings or self.output_format != "PNG":
            raise ValueError("Render overrides are not supported; only PNG results are supported")

    def apply_to_job(self, job: RenderJob) -> RenderJob:
        """Apply template settings to a job."""
        self.check_supported()
        job.priority = self.priority
        job.requires_gpu = self.requires_gpu
        job.min_memory_gb = self.min_memory_gb
        job.worker_tags = self.worker_tags.copy()
        return job


class JobTemplateManager:
    """Manages job templates."""

    def __init__(self, templates_dir: Path):
        """
        Initialize template manager.

        Args:
            templates_dir: Directory to store templates
        """
        self.templates_dir = templates_dir
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self._templates: Dict[str, JobTemplate] = {}
        self._load_templates()

    def _load_templates(self) -> None:
        """Load templates from disk."""
        for template_file in self.templates_dir.glob("*.json"):
            try:
                with open(template_file, "r") as f:
                    data = json.load(f)
                    template = JobTemplate(**data)
                    self._templates[template.id] = template
            except Exception as e:
                print(f"⚠️  Failed to load template {template_file}: {e}")

    def save_template(self, template: JobTemplate) -> None:
        """Save a template to disk."""
        template_file = self.templates_dir / f"{template.id}.json"
        with open(template_file, "w") as f:
            json.dump(template.model_dump(mode="json"), f, indent=2)
        self._templates[template.id] = template

    def get_template(self, template_id: str) -> Optional[JobTemplate]:
        """Get a template by ID."""
        return self._templates.get(template_id)

    def list_templates(self) -> list[JobTemplate]:
        """List all templates."""
        return list(self._templates.values())

    def delete_template(self, template_id: str) -> bool:
        """Delete a template."""
        if template_id not in self._templates:
            return False

        template_file = self.templates_dir / f"{template_id}.json"
        if template_file.exists():
            template_file.unlink()

        del self._templates[template_id]
        return True

    def create_from_preset(self, preset: JobPreset) -> JobTemplate:
        """Create and save a template from a preset."""
        template = JobTemplate.from_preset(preset)
        self.save_template(template)
        return template


class JobExporter:
    """Exports and imports job configurations."""

    @staticmethod
    def export_job(job: RenderJob, include_tasks: bool = False) -> Dict[str, Any]:
        """
        Export job configuration to dictionary.

        Args:
            job: Job to export
            include_tasks: Whether to include task details

        Returns:
            Dictionary with job configuration
        """
        data = {
            "id": job.id,
            "name": job.name,
            "frame_start": job.frame_start,
            "frame_end": job.frame_end,
            "priority": job.priority,
            "requires_gpu": job.requires_gpu,
            "min_memory_gb": job.min_memory_gb,
            "worker_tags": list(job.worker_tags),
            "depends_on": list(job.depends_on),
            "scheduled_time": job.scheduled_time.isoformat() if job.scheduled_time else None,
            "output_directory": str(job.output_directory),
            "blender_version": job.blender_version,
            "created_at": job.created_at.isoformat(),
        }

        if include_tasks:
            data["tasks"] = [
                {
                    "frame_number": t.frame_number,
                    "status": t.status.value,
                    "render_time_seconds": t.render_time_seconds,
                }
                for t in job.tasks
            ]

        return data

    @staticmethod
    def export_to_file(job: RenderJob, file_path: Path, include_tasks: bool = False) -> None:
        """Export job to JSON file."""
        data = JobExporter.export_job(job, include_tasks)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def import_from_file(file_path: Path) -> Dict[str, Any]:
        """Import job configuration from JSON file."""
        with open(file_path, "r") as f:
            return json.load(f)

    @staticmethod
    def create_job_from_config(config: Dict[str, Any], blend_file: Path, *, create_tasks=True) -> RenderJob:
        """
        Create a job from exported configuration.

        Args:
            config: Job configuration dictionary
            blend_file: Path to the blend file

        Returns:
            New RenderJob instance
        """
        if config.get("render_settings") or config.get("output_format", "PNG") != "PNG":
            raise ValueError("Render overrides are not supported; only PNG results are supported")
        job = RenderJob(
            name=config.get("name", ""),
            blend_file=blend_file,
            frame_start=config["frame_start"],
            frame_end=config["frame_end"],
            priority=config.get("priority", 5),
            requires_gpu=config.get("requires_gpu", False),
            min_memory_gb=config.get("min_memory_gb"),
            worker_tags=config.get("worker_tags", []),
            depends_on=config.get("depends_on", []),
            scheduled_time=config.get("scheduled_time"),
            blender_version=config.get("blender_version", ""),
        )

        # Parse created_at if present
        if "created_at" in config:
            try:
                job.created_at = datetime.fromisoformat(config["created_at"])
            except Exception:
                pass

        if create_tasks:
            job.create_tasks()
        return job
