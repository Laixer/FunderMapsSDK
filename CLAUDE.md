# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FunderMaps SDK is an async Python toolkit for ETL operations on the FunderMaps geospatial platform. It provides service providers for PostgreSQL/PostGIS databases, S3-compatible storage, GDAL geospatial processing, email (Mailgun), and PDF generation (PDF.co). CLI scripts orchestrate mapset processing, job queues, data loading, and product exports.

## Common Commands

```bash
# Install dependencies
uv sync              # production
uv sync --dev        # with dev tools

# Run CLI scripts
uv run process_mapset.py --tileset analysis_foundation
uv run process_worker_jobs.py --poll-interval 30
uv run refresh_models.py
uv run export_product.py
uv run load_dataset.py /path/to/dataset.csv

# Lint and format
ruff check .         # lint
ruff format .        # format

# Type check
mypy fundermapssdk

# Tests
pytest
pytest -m "not slow"
pytest tests/test_specific.py::test_name   # single test

# Docker
docker build -t fundermaps-sdk .
docker run --env-file .env fundermaps-sdk process_mapset.py
```

## Architecture

**`fundermapssdk/`** — Core SDK package with lazy-initialized service providers:
- `__init__.py` — `FunderMapsSDK` main entry point, factory for all providers
- `command.py` — `FunderMapsCommand` base class for CLI scripts with argument parsing, env loading, SDK init, and lifecycle hooks (`pre_execute` → `execute` → `post_execute`)
- `db.py` — `DbProvider` (PostgreSQL/PostGIS) — context manager, SQL script execution, view refresh, table management
- `gdal.py` — `GDALProvider` — async GDAL/OGR operations (`from_postgis`, `to_postgis`, `ogr2ogr`)
- `storage.py` — `ObjectStorageProvider` — S3-compatible storage with ThreadPoolExecutor for parallel uploads
- `mail.py` — `MailProvider` (Mailgun API)
- `pdf.py` — `PDFProvider` (PDF.co API, async)
- `config.py` — Dataclass-based configs (`DatabaseConfig`, `S3Config`, `PDFCoConfig`, `MailConfig`)

**Root-level CLI scripts** inherit from `FunderMapsCommand` and implement `execute()`:
- `process_mapset.py` — PostGIS → GPKG → GeoJSON → Tippecanoe tiles → S3 upload
- `process_worker_jobs.py` — Long-running job queue poller with concurrent execution
- `refresh_models.py` — Refresh materialized views and reindex tables
- `export_product.py`, `generate_pdf.py`, `load_dataset.py`, `cleanup_storage.py`, `send_mail.py`

## Key Patterns

- **Async/await** for I/O-bound operations (GDAL, Tippecanoe, PDF generation)
- **Context managers** for resource lifecycle (DB connections, S3 clients)
- **Environment-based config** loaded from `.env`, `.env.local`, or `/etc/fundermaps/config.env`
- All CLI scripts use `FunderMapsCommand` which handles argparse, logging, and SDK initialization

## Tooling

- **Python 3.12** (`.python-version`)
- **uv** package manager (`uv.lock`)
- **Ruff** for linting and formatting
- **MyPy** strict mode for type checking (exemptions for boto3, mailgun, psycopg2)
- **pytest** with markers: `slow`, `integration`; coverage via pytest-cov

## External Dependencies

Runtime requires GDAL/OGR libraries and Tippecanoe (compiled from source in Docker). The Dockerfile uses a multi-stage build to compile Tippecanoe then copies it to a Debian stable-slim runtime image.
