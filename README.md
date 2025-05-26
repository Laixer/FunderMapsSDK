# FunderMaps SDK

A Python SDK for handling Extract, Transform, Load (ETL) operations for the FunderMaps platform. This toolkit runs on worker servers to manage geospatial data processing, storage operations, and product exports.

## Overview

FunderMaps SDK provides a collection of scripts and utilities for:

- Processing geospatial data (GIS operations via GDAL)
- Managing map tilesets (via Tippecanoe)
- Handling database operations (PostgreSQL/PostGIS)
- Object storage management (S3)
- Product exports and reporting
- Cleaning up orphaned storage resources

## Prerequisites

- Python 3.10+
- PostgreSQL with PostGIS extension
- GDAL/OGR libraries
- Tippecanoe for vector tile generation
- AWS S3 or compatible object storage

## Installation

1. Clone this repository
2. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Key Components

### Command Line Tools

The SDK provides several command-line tools:

- **process_mapset.py**: Generates and uploads map tilesets to S3
- **export_product.py**: Exports product data for organizations
- **cleanup_storage.py**: Removes orphaned files from storage
- **refresh_models.py**: Refreshes database models
- **load_dataset.py**: Loads new datasets into the database

### Core SDK Components

- **Database Provider**: Interface for PostgreSQL operations
- **GDAL Provider**: Geospatial data processing
- **Object Storage Provider**: S3 storage operations
- **Tippecanoe**: Vector tile generation utilities

## Usage Examples

### Processing Map Tilesets

```bash
python process_mapset.py --tileset analysis_foundation analysis_report --max-workers 3
```

### Exporting Product Data

```bash
python export_product.py
```

### Cleaning Up Storage

```bash
python cleanup_storage.py
```

### Load Dataset

```bash
python load_dataset.py /path/to/dataset.csv
```

## Configuration

The SDK uses environment variables or configuration files for:

- Database connection parameters
- S3 credentials and endpoints
- GDAL configuration

Example configuration in code:

```python
from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config

sdk = FunderMapsSDK(
    db_config=DatabaseConfig(
        host="localhost",
        port=5432,
        database="fundermaps",
        user="postgres",
        password="password"
    ),
    s3_config=S3Config(
        access_key="your-access-key",
        secret_key="your-secret-key",
        endpoint="s3.amazonaws.com"
    )
)
```

## Deployment

The SDK includes systemd service files in the `contrib/` directory for deploying the scripts as scheduled services on a Linux server:

- `fundermaps-export-product.service` & `.timer`
- `fundermaps-process-mapset.service`
- `fundermaps-refresh-model.service` & `.timer`

## Project Structure

```
FunderMapsSDK/
├── fundermapssdk/           # Core SDK modules
│   ├── __init__.py          # Main SDK entry point
│   ├── cli.py               # Command line utilities
│   ├── config.py            # Configuration management
│   ├── db.py                # Database providers
│   ├── gdal.py              # Geospatial data utilities
│   ├── storage.py           # S3 storage utilities
│   └── tippecanoe.py        # Vector tile generation
├── contrib/                 # Deployment files
├── scripts/                 # Helper scripts
├── sql/                     # SQL queries for data loading
├── cleanup_storage.py       # Storage cleanup script
├── export_product.py        # Product export script
├── process_mapset.py        # Map tileset processing
└── requirements.txt         # Python dependencies
```

## License

See the LICENSE file for details.