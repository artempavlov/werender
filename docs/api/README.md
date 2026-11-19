# WeRender API Reference

This document provides an overview of the WeRender HTTP API.

## Base URL

The Coordinator exposes its API at:

```text
http://<coordinator-ip>:8420
```

## Authentication

Protected endpoints require an API key in the `X-API-Key` header. Use the coordinator
key for job management and dashboard polling, and the worker key for render task requests:

```http
X-API-Key: your_worker_api_key
```

See the [Security Guide](../security/SECURITY_GUIDE.md) for setup details.

---

## HTTP Endpoints

### Jobs

| Method   | Endpoint                   | Description               |
| -------- | -------------------------- | ------------------------- |
| `GET`    | `/api/jobs`                | List all jobs             |
| `POST`   | `/api/jobs/create`         | Create a job with multipart fields `file`, `name` (optional), `frame_start`, and `frame_end` |
| `GET`    | `/api/jobs/{job_id}`       | Get job details           |
| `POST`   | `/api/jobs/{job_id}/start` | Start a job               |
| `POST`   | `/api/jobs/{job_id}/pause` | Pause a job               |
| `POST`   | `/api/jobs/{job_id}/cancel` | Cancel a job             |
| `GET`    | `/api/jobs/{job_id}/blend` | Download blend file (auth required) |

### Tasks

| Method | Endpoint                       | Auth | Description              |
| ------ | ------------------------------ | ---- | ------------------------ |
| `GET`  | `/api/tasks/request`           | ✅   | Request next available task |
| `POST` | `/api/tasks/{task_id}/complete`| ✅   | Upload completed frame   |
| `POST` | `/api/tasks/{task_id}/fail`    | ✅   | Report task failure      |

### Workers

| Method | Endpoint       | Description            |
| ------ | -------------- | ---------------------- |
| `GET`  | `/api/workers` | List connected workers |

---

## Dashboard

The web dashboard is served at the root URL:

```text
http://<coordinator-ip>:8420/
```

The dashboard polls the HTTP API. Enter a coordinator key or use an authenticated
reverse proxy that injects the appropriate key server-side without exposing it to the browser.
