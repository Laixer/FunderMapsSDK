# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FunderMaps Worker is an async Python toolkit for ETL operations on the FunderMaps geospatial platform. It provides service providers for PostgreSQL/PostGIS databases, S3-compatible storage, GDAL geospatial processing, email (Mailgun), and PDF generation (PDF.co). A long-running job queue worker dispatches commands for mapset processing, data loading, model refresh, product exports, and more.

## Common Commands

```bash
# Install dependencies
uv sync              # production
uv sync --dev        # with dev tools

# Run the job queue worker
uv run process_worker_jobs.py --poll-interval 30

# Run individual commands directly
uv run -m fundermapsworker.commands.process_mapset -- --tileset analysis_foundation
uv run -m fundermapsworker.commands.refresh_models
uv run -m fundermapsworker.commands.export_product
uv run -m fundermapsworker.commands.load_dataset -- /path/to/dataset.csv

# Lint and format
ruff check .         # lint
ruff format .        # format

# Type check
mypy fundermapsworker

# Tests
pytest
pytest -m "not slow"
pytest tests/test_specific.py::test_name   # single test

# Container (Podman)
podman build -t fundermaps-worker .
podman run --env-file .env fundermaps-worker process_worker_jobs.py
```

## Architecture

### Package: `fundermapsworker/`

**Core:**
- `__init__.py` — `FunderMapsWorker` main entry point, factory for all providers
- `command.py` — `WorkerCommand` base class for CLI commands with argument parsing, env loading, worker init, and lifecycle hooks (`pre_execute` → `execute` → `post_execute`)
- `config.py` — Dataclass-based configs (`DatabaseConfig`, `S3Config`, `PDFCoConfig`, `MailConfig`)
- `util.py` — File validation helpers

**`providers/`** — Service providers (lazy-initialized):
- `db.py` — `DbProvider` (PostgreSQL/PostGIS) — context manager, SQL script execution, view refresh, table management. Uses TCP keepalive.
- `gdal.py` — `GDALProvider` — async GDAL/OGR operations (`from_postgis`, `to_postgis`, `ogr2ogr`)
- `storage.py` — `ObjectStorageProvider` — S3-compatible storage with ThreadPoolExecutor for parallel uploads
- `tippecanoe.py` — `TippecanoeProvider` — vector tile generation
- `mail.py` — `MailProvider` (Mailgun API)
- `pdf.py` — `PDFProvider` (PDF.co API, async)

**`commands/`** — CLI commands (inherit from `WorkerCommand`):
- `process_mapset.py` — PostGIS → GPKG → GeoJSON → Tippecanoe tiles → S3 upload
- `refresh_models.py` — Refresh materialized views and reindex tables
- `export_product.py` — Monthly product data export
- `load_dataset.py` — Load geospatial datasets via GDAL
- `generate_pdf.py` — PDF generation via PDF.co
- `cleanup_storage.py` — Remove orphaned S3 objects
- `send_mail.py` — Send email via Mailgun

**Root-level:**
- `process_worker_jobs.py` — Long-running job queue poller that dispatches to commands above

### Job Queue Architecture

All scheduled work flows through the `application.worker_jobs` PostgreSQL table:
- Systemd timers submit jobs via `INSERT INTO application.worker_jobs`
- The worker service (`process_worker_jobs.py`) polls the queue and dispatches to commands
- `refresh_models` automatically chains `process_mapset` on success
- Jobs have retry semantics with exponential backoff

### SQL Directory: `sql/`

- `sql/model/` — Risk model pipeline (helper functions, precomputed tables, matview definitions, statistics fixes)
- `sql/load/` — Data loading scripts (BAG buildings/addresses, subsidence)
- `sql/migrate/` — One-off migration scripts

## Key Patterns

- **Async/await** for I/O-bound operations (GDAL, Tippecanoe, PDF generation)
- **Context managers** for resource lifecycle (DB connections, S3 clients)
- **Environment-based config** loaded from `.env`, `.env.local`, or `/etc/fundermaps/config.env`
- All commands use `WorkerCommand` which handles argparse, logging, and worker initialization

## Tooling

- **Python 3.12** (`.python-version`)
- **uv** package manager (`uv.lock`)
- **Ruff** for linting and formatting
- **MyPy** strict mode for type checking (exemptions for boto3, mailgun, psycopg2)
- **pytest** with markers: `slow`, `integration`; coverage via pytest-cov

## External Dependencies

Runtime requires GDAL/OGR libraries and Tippecanoe (compiled from source in container). The Containerfile uses a multi-stage build to compile Tippecanoe then copies it to a Debian stable-slim runtime image.
