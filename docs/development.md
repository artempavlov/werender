# Development Guide

## Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/toxzak-svg/werender.git
   cd werender
   ```

2. **Create a Virtual Environment**

   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux/Mac
   source .venv/bin/activate
   ```

3. **Install in Editable Mode**
   This installs the package with all development dependencies (pytest, ruff, etc.).

   ```bash
   pip install -e ".[dev]"
   ```

## Project Structure

- `src/werender/`
  - `core/`: Core logic for Job, Node (Worker/Coordinator), and Configuration management.
  - `network/`: HTTP server/client implementations and mDNS discovery.
  - `dashboard/`: React/Static files for the web interface.
  - `addons/`: The Blender add-on script (`werender_submitter.py`) used inside Blender.
  - `scheduler/`: Logic for assigning frames to workers.

## Running Tests

We use `pytest` for testing.
Use a disposable home directory so tests do not read or change live keys or job state.

```bash
mkdir -p .tmp/test-home
HOME="$PWD/.tmp/test-home" python -m pytest

# Run a specific test file
HOME="$PWD/.tmp/test-home" python -m pytest tests/test_discovery.py
```

## Code Style

We use `ruff` for linting and formatting.

```bash
# Check for issues
ruff check .

# Auto-fix issues
ruff check --fix .
```

## Building

To build the package for distribution:

```bash
python -m pip install build twine
python -m build --outdir .tmp/package-check
python -m twine check --strict .tmp/package-check/*
```

Hatchling builds the source archive and builds the wheel from that archive.
The wheel includes the dashboard and Blender add-on. Use a new output directory for
each release check so stale archives cannot be mistaken for the new build. Do not commit
generated archives.
