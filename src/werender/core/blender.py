"""Blender CLI wrapper for WeRender."""

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


RENDER_FORMATS = {
    "PNG": ".png",
    "JPEG": ".jpg",
    "OPEN_EXR": ".exr",
    "OPEN_EXR_MULTILAYER": ".exr",
    "TIFF": ".tif",
    "BMP": ".bmp",
    "TARGA": ".tga",
    "TARGA_RAW": ".tga",
}


def normalize_output_format(output_format: str) -> str:
    output_format = output_format.upper()
    if output_format == "EXR":
        output_format = "OPEN_EXR"
    if output_format not in RENDER_FORMATS:
        raise ValueError(f"Unsupported render output format: {output_format}")
    return output_format


def _render_settings_expression(settings: dict) -> str:
    lines = ["import bpy", "scene = bpy.context.scene", "scene.render.use_file_extension = True"]
    for name, value in settings.items():
        if name not in {"resolution_percentage", "samples"}:
            raise ValueError(f"Unsupported render setting: {name}")
        limit = 100 if name == "resolution_percentage" else 4096
        if type(value) is not int or not 1 <= value <= limit:
            raise ValueError(f"{name} must be an integer from 1 to {limit}")
        if name == "resolution_percentage":
            lines.append(f"scene.render.resolution_percentage = {value:d}")
        else:
            lines.extend([
                "if scene.render.engine == 'CYCLES':",
                f"    scene.cycles.samples = {value:d}",
                "elif scene.render.engine in {'BLENDER_EEVEE', 'BLENDER_EEVEE_NEXT'}:",
                "    if hasattr(scene.eevee, 'taa_render_samples'):",
                f"        scene.eevee.taa_render_samples = {value:d}",
                "    else:",
                f"        scene.eevee.taa_samples = {value:d}",
            ])
    return "\n".join(lines)


@dataclass
class RenderResult:
    """Result of a render operation."""

    success: bool
    """Whether the render succeeded."""

    frame: int
    """Frame number that was rendered."""

    output_file: Optional[Path] = None
    """Path to the output file if successful."""

    render_time: float = 0.0
    """Time taken to render in seconds."""

    error_message: Optional[str] = None
    """Error message if render failed."""


class BlenderNotFoundError(Exception):
    """Raised when Blender executable cannot be found."""


class BlenderVersionError(Exception):
    """Raised when Blender version is incompatible."""


class BlenderRenderer:
    """Wrapper for Blender command-line rendering."""

    def __init__(self, blender_path: str = "blender"):
        """
        Initialize the Blender renderer.

        Args:
            blender_path: Path to Blender executable. Defaults to 'blender'
                         which assumes it's available in system PATH.
        """
        self.blender_path = self._find_blender(blender_path)
        self._version: Optional[str] = None

    def _find_blender(self, blender_path: str) -> str:
        """
        Find the Blender executable.

        Args:
            blender_path: User-provided path or 'blender' for PATH lookup.

        Returns:
            Absolute path to Blender executable.

        Raises:
            BlenderNotFoundError: If Blender cannot be found.
        """
        # Check if it's a direct path
        if Path(blender_path).exists():
            return str(Path(blender_path).resolve())

        # Try to find in PATH
        found = shutil.which(blender_path)
        if found:
            return found

        # Common installation paths on Windows
        common_paths = [
            Path("C:/Program Files/Blender Foundation"),
            Path("C:/Program Files (x86)/Blender Foundation"),
            Path.home() / "AppData/Roaming/Blender Foundation",
        ]

        for base_path in common_paths:
            if base_path.exists():
                # Find latest Blender version directory
                blender_dirs = sorted(
                    [d for d in base_path.iterdir() if d.is_dir() and d.name.startswith("Blender")],
                    reverse=True,
                )
                for blender_dir in blender_dirs:
                    exe_path = blender_dir / "blender.exe"
                    if exe_path.exists():
                        return str(exe_path)

        raise BlenderNotFoundError(
            f"Could not find Blender at '{blender_path}' or in common installation paths. "
            "Please install Blender or provide the full path to the executable."
        )

    def get_version(self) -> str:
        """
        Get the Blender version.

        Returns:
            Version string like "4.0.0".

        Raises:
            BlenderVersionError: If version cannot be determined.
        """
        if self._version:
            return self._version

        try:
            result = subprocess.run(
                [self.blender_path, "--version"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            # Parse version from output like "Blender 4.0.0"
            match = re.search(r"Blender (\d+\.\d+\.\d+)", result.stdout)
            if match:
                self._version = match.group(1)
                return self._version

            raise BlenderVersionError(f"Could not parse version from: {result.stdout}")

        except subprocess.TimeoutExpired:
            raise BlenderVersionError("Blender version check timed out")
        except Exception as e:
            raise BlenderVersionError(f"Failed to get Blender version: {e}")

    def render_frame(
        self,
        blend_file: Path,
        frame: int,
        output_dir: Path,
        output_format: str = "PNG",
        render_settings: Optional[dict] = None,
    ) -> RenderResult:
        """
        Render a single frame from a .blend file.

        Args:
            blend_file: Path to the .blend file.
            frame: Frame number to render.
            output_dir: Directory to save the rendered frame.
            output_format: Output format (PNG, JPEG, EXR, etc.).

        Returns:
            RenderResult with success status and output file path.
        """
        import time

        blend_file = Path(blend_file).resolve()
        output_dir = Path(output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        start_time = time.time()

        try:
            output_format = normalize_output_format(output_format)
            expected_output = output_dir / f"{frame:04d}{RENDER_FORMATS[output_format]}"
            with tempfile.TemporaryDirectory(prefix=".render-", dir=output_dir) as directory:
                staging = Path(directory)
                cmd = [self.blender_path, "-b", "--disable-autoexec", str(blend_file),
                       "--python-exit-code", "1"]
                if gpu_setup := os.getenv("WERENDER_GPU_SETUP") or os.getenv("WERENDER_CUDA_SETUP"):
                    cmd.extend(["--python", gpu_setup])
                cmd.extend([
                    "--python-expr", _render_settings_expression(render_settings or {}),
                    "-o", str(staging / "####"), "-F", output_format, "-f", str(frame),
                ])
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                render_time = time.time() - start_time
                for line in result.stdout.splitlines():
                    if line.startswith(("WERENDER_CUDA:", "WERENDER_HIP:")):
                        print(line, flush=True)

                if result.returncode != 0:
                    return RenderResult(False, frame, render_time=render_time, error_message=(
                        f"Blender exited with code {result.returncode}: "
                        f"{result.stderr[-2000:] or result.stdout[-2000:]}"
                    ))
                staged_file = staging / expected_output.name
                if not staged_file.is_file() or not staged_file.stat().st_size:
                    return RenderResult(False, frame, render_time=render_time,
                                        error_message=f"Blender did not produce {expected_output.name}")
                staged_file.replace(expected_output)
                return RenderResult(True, frame, expected_output, render_time)

        except subprocess.TimeoutExpired:
            return RenderResult(
                success=False,
                frame=frame,
                render_time=time.time() - start_time,
                error_message="Render timed out after 1 hour",
            )
        except Exception as e:
            return RenderResult(
                success=False,
                frame=frame,
                error_message=str(e),
            )

    def pack_resources(self, blend_file: Path, output_path: Optional[Path] = None) -> Path:
        """
        Pack all external resources into the .blend file.

        This creates a copy of the file with all textures, images, and other
        external resources embedded, eliminating path dependency issues.

        Args:
            blend_file: Path to the original .blend file.
            output_path: Optional path for the packed file. If None, creates
                        a temp file with '_packed' suffix.

        Returns:
            Path to the packed .blend file.

        Raises:
            RuntimeError: If packing fails.
        """
        blend_file = Path(blend_file).resolve()

        if output_path is None:
            with tempfile.NamedTemporaryFile(prefix="werender-packed-", suffix=".blend", delete=False) as placeholder:
                output_path = Path(placeholder.name)
            output_path.unlink()
        output_path = Path(output_path).resolve()

        # Python script to run inside Blender
        pack_script = """
import bpy
import sys

if 'FINISHED' not in bpy.ops.file.pack_all():
    raise RuntimeError("Blender could not pack resources")
if 'FINISHED' not in bpy.ops.wm.save_as_mainfile(filepath=sys.argv[-1], relative_remap=False):
    raise RuntimeError("Blender could not save packed project")
"""

        try:
            with tempfile.TemporaryDirectory(prefix=".pack-", dir=output_path.parent) as directory:
                script_path = Path(directory) / "pack.py"
                script_path.write_text(pack_script, encoding="utf-8")
                staging = Path(directory) / "packed.blend"
                result = subprocess.run(
                    [self.blender_path, "-b", "--disable-autoexec", str(blend_file),
                     "--python-exit-code", "1", "--python", str(script_path), "--", str(staging)],
                    capture_output=True, text=True, timeout=300,
                )
                if result.returncode == 0 and staging.is_file() and staging.stat().st_size:
                    staging.replace(output_path)
                    return output_path
                raise RuntimeError(f"Failed to pack resources: {result.stderr or result.stdout}")

        except subprocess.TimeoutExpired:
            raise RuntimeError("Packing resources timed out after 5 minutes")

    def _format_to_extension(self, output_format: str) -> str:
        """Convert Blender format name to file extension."""
        return RENDER_FORMATS[normalize_output_format(output_format)]
