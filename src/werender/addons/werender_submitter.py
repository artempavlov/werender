import json
import os
import tempfile
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from pathlib import Path
from uuid import uuid4

import bpy

bl_info = {
    "name": "WeRender Submitter",
    "author": "WeRender Team",
    "version": (0, 2, 0),
    "blender": (3, 0, 0),
    "location": "Properties > Render > WeRender",
    "description": "Submit render jobs to a remote WeRender Coordinator",
    "category": "Render",
}


class SubmissionError(Exception):
    """A submission failure with an actionable message for the artist."""


def upload_job(host, port, use_https, api_key, blend_path, job_name, start, end):
    boundary = uuid4().hex
    prefix = b"".join(
        f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
        f'{value}\r\n'.encode("utf-8")
        for name, value in (("name", job_name), ("frame_start", start), ("frame_end", end))
    )
    prefix += (
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="scene.blend"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
    ).encode("ascii")
    suffix = f"\r\n--{boundary}--\r\n".encode("ascii")

    connection_type = HTTPSConnection if use_https else HTTPConnection
    connection = connection_type(host, port, timeout=30)
    try:
        with blend_path.open("rb") as blend:
            def body():
                yield prefix
                yield from iter(lambda: blend.read(64 * 1024), b"")
                yield suffix

            connection.request("POST", "/api/jobs/create", body=body(), headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(len(prefix) + blend_path.stat().st_size + len(suffix)),
                "X-API-Key": api_key,
            })
            with connection.getresponse() as response:
                status = response.status
                payload = response.read(64 * 1024)
    except TimeoutError as exc:
        raise SubmissionError("Upload timed out. Check the job list before retrying.") from exc
    except (HTTPException, OSError) as exc:
        raise SubmissionError("Check the coordinator host, port, and HTTPS setting.") from exc
    finally:
        connection.close()

    if status in (401, 403):
        raise SubmissionError("Access denied. Check the coordinator API key and endpoint.")
    if 300 <= status < 400:
        raise SubmissionError("Upload redirected. Check the coordinator HTTPS and port settings.")
    try:
        result = json.loads(payload)
    except (ValueError, UnicodeError):
        result = None
    if 200 <= status < 300 and isinstance(result, dict) and result.get("job_id"):
        return result["job_id"]
    detail = result.get("detail") if isinstance(result, dict) else None
    if isinstance(detail, list):
        detail = "; ".join(str(item.get("msg", "Invalid field")) for item in detail
                           if isinstance(item, dict))
    if isinstance(detail, str) and detail:
        raise SubmissionError(f"Coordinator rejected the upload (HTTP {status}): {detail[:300]}")
    if 200 <= status < 300:
        raise SubmissionError("Unexpected response. Check the job list before retrying.")
    raise SubmissionError(f"Upload failed (HTTP {status}). Check the coordinator logs.")


class WeRenderPreferences(bpy.types.AddonPreferences):
    bl_idname = __name__

    hub_address: bpy.props.StringProperty(
        name="Coordinator Host", description="Host name or IP address (no scheme or path)",
        default="127.0.0.1",
    )
    hub_port: bpy.props.IntProperty(name="Coordinator Port", default=8420, min=1, max=65535)
    use_https: bpy.props.BoolProperty(name="Use HTTPS", default=False)
    api_key: bpy.props.StringProperty(
        name="Coordinator API Key", subtype="PASSWORD",
        description="Stored in Blender preferences if saved; leave blank to read "
                    "WERENDER_COORDINATOR_API_KEY from Blender's environment",
    )

    def draw(self, context):
        for name in ("hub_address", "hub_port", "use_https", "api_key"):
            self.layout.prop(self, name)


class WeRenderSettings(bpy.types.PropertyGroup):
    job_name: bpy.props.StringProperty(name="Job Name", description="Defaults to the file name")
    auto_pack: bpy.props.BoolProperty(
        name="Pack Resources in Current Scene", default=False,
        description="Also packs resources into your open scene in memory; "
                    "does not overwrite the original file",
    )
    status_message: bpy.props.StringProperty(name="Status", default="Ready", options={"SKIP_SAVE"})


class WERENDER_OT_submit_job_async(bpy.types.Operator):
    """Submit a copy of the current scene to the WeRender coordinator"""

    # Keep the existing operator ID for installed keymaps.
    bl_idname = "werender.submit_job_async"
    bl_label = "Submit Job"

    def execute(self, context):
        scene = context.scene
        settings = scene.werender_settings
        prefs = context.preferences.addons[__name__].preferences
        original = bpy.data.filepath
        api_key = (prefs.api_key or os.environ.get("WERENDER_COORDINATOR_API_KEY", "")).strip()
        host = prefs.hub_address.strip()

        try:
            if not original:
                raise SubmissionError("Save the .blend file before submitting it.")
            if not api_key:
                raise SubmissionError("Set the coordinator API key in the add-on preferences.")
            if any(not 33 <= ord(char) <= 126 for char in api_key):
                raise SubmissionError("Check the coordinator API key for invalid characters.")
            if not host or any(char.isspace() or char in "/@?#" for char in host):
                raise SubmissionError("Enter a coordinator host name or IP address, without a URL.")
            with tempfile.TemporaryDirectory(prefix="werender-") as directory:
                blend = Path(directory) / "scene.blend"
                if settings.auto_pack and 'FINISHED' not in bpy.ops.file.pack_all():
                    raise SubmissionError("Resource packing failed. Check external files.")
                saved = bpy.ops.wm.save_as_mainfile(filepath=str(blend), copy=True)
                if 'FINISHED' not in saved or not blend.is_file():
                    raise SubmissionError("Blender could not save the temporary .blend copy.")

                # Blender does not support background Python threads in add-ons.
                job_id = upload_job(
                    host, prefs.hub_port, prefs.use_https, api_key, blend,
                    settings.job_name.strip() or bpy.path.basename(original),
                    scene.frame_start, scene.frame_end,
                )
        except SubmissionError as exc:
            settings.status_message = str(exc)
            self.report({'ERROR'}, str(exc))
            return {'CANCELLED'}
        except (RuntimeError, OSError, ValueError):
            message = "Could not prepare or send the copy. Check temporary disk space and settings."
            settings.status_message = message
            self.report({'ERROR'}, message)
            return {'CANCELLED'}

        settings.status_message = f"Job submitted: {job_id}"
        self.report({'INFO'}, settings.status_message)
        return {'FINISHED'}


class WERENDER_PT_panel(bpy.types.Panel):
    bl_label = "WeRender"
    bl_idname = "WERENDER_PT_panel"
    bl_space_type = 'PROPERTIES'
    bl_region_type = 'WINDOW'
    bl_context = "render"

    def draw(self, context):
        layout = self.layout
        settings = context.scene.werender_settings
        prefs = context.preferences.addons[__name__].preferences
        scheme = "https" if prefs.use_https else "http"
        layout.label(text=f"Coordinator: {scheme}://{prefs.hub_address}:{prefs.hub_port}")
        layout.prop(settings, "job_name")
        layout.prop(settings, "auto_pack")
        row = layout.row(align=True)
        row.prop(context.scene, "frame_start")
        row.prop(context.scene, "frame_end")
        layout.operator("werender.submit_job_async", icon='RENDER_ANIMATION')
        layout.label(text=settings.status_message)


classes = (WeRenderPreferences, WeRenderSettings, WERENDER_OT_submit_job_async, WERENDER_PT_panel)


def register():
    for cls in classes:
        bpy.utils.register_class(cls)
    bpy.types.Scene.werender_settings = bpy.props.PointerProperty(type=WeRenderSettings)


def unregister():
    del bpy.types.Scene.werender_settings
    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)


if __name__ == "__main__":
    register()
