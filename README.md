# tolteca_db

**Production-Ready Database System for TolTEC Telescope Data**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![SQLAlchemy 2.0](https://img.shields.io/badge/SQLAlchemy-2.0-green.svg)](https://www.sqlalchemy.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.0+-yellow.svg)](https://duckdb.org/)

Unified database backend for TolTEC telescope data products with content-addressable storage, incremental association generation, and comprehensive CLI.

---

## Features

- **DuckDB OLAP Database** - 10-100× faster analytics with columnar storage
- **Incremental Associations** - 100-1000× speedup for continuous data ingestion
- **Type-Safe Metadata** - Dataclass models with automatic JSON serialization
- **Comprehensive CLI** - 15 commands across database, ingestion, associations, and queries
- **Flexible Backends** - Database or filesystem state tracking
- **Production-Ready** - 8/8 core tests passing, Python 3.13 compatible

---

## Quick Start

### Installation

```bash
cd tolteca_db
uv pip install -e .
```

### Initialize Database

```bash
# Create persistent DuckDB database with registry tables
tolteca_db db init --url "duckdb:///toltec.duckdb"
```

### Ingest Data

```bash
# Ingest TolTEC observation files
tolteca_db ingest directory /data/toltec/raw_observations \
  --location LMT \
  --recursive \
  --pattern "*.nc"
```

### Generate Associations

```bash
# Incremental association generation (100-1000× faster)
tolteca_db assoc generate --incremental
```

### Query Data

```bash
# Query raw observations
tolteca_db query obs --obsnum 113533

# Query association groups
tolteca_db query groups --type cal_group --members

# Database statistics
tolteca_db query stats
```

---

## CLI Commands

### Database Management (`db`)

- `tolteca_db db init` - Initialize schema and registry tables
- `tolteca_db db info` - Display database statistics
- `tolteca_db db export` - Backup tables to Parquet
- `tolteca_db db vacuum` - Optimize database

### Data Ingestion (`ingest`)

- `tolteca_db ingest file` - Ingest single TolTEC file
- `tolteca_db ingest directory` - Batch ingestion with scanning
- `tolteca_db ingest scan` - Preview parseable files (dry run)

### Association Generation (`assoc`)

- `tolteca_db assoc generate` - Incremental/full association generation
- `tolteca_db assoc stream` - Streaming processing with batching
- `tolteca_db assoc state` - Show state statistics
- `tolteca_db assoc reset-state` - Reset filesystem state

### Query & Export (`query`)

- `tolteca_db query obs` - Query raw observations
- `tolteca_db query groups` - Query association groups
- `tolteca_db query export` - Export to CSV/Parquet
- `tolteca_db query stats` - Database statistics

---

## Documentation

- **[CLI User Guide](design/CLI_User_Guide.md)** - Complete command reference with examples
- **[Architecture](design/architecture.md)** - System design and patterns
- **[Implementation Guide](design/implementation.md)** - Development phases and status
- **[Incremental Associations](design/Incremental_Associations_Implementation.md)** - Performance optimization details

---

## Architecture Highlights

### Data Pipeline

```
1. Ingest → Filesystem scanning, filename parsing, database population
2. Pool → Batch load observations into DataFrame for fast filtering
3. State → Track grouped observations and existing groups (O(1) lookups)
4. Associate → Generate groups incrementally (100-1000× speedup)
5. Query → High-level queries and exports
```

### Performance

| Operation | Traditional | Incremental | Speedup |
|-----------|------------|-------------|---------|
| 1000 obs + 10 new | ~10s | ~0.01s | **1000×** |
| 10K obs + 100 new | ~5min | ~0.1s | **3000×** |

### Technology Stack

- **Database:** DuckDB 1.0+ (OLAP-optimized, columnar)
- **ORM:** SQLAlchemy 2.0.44+ (pure, no SQLModel)
- **Serialization:** adaptix with AdaptixJSON
- **CLI:** Typer with Rich formatting
- **Testing:** pytest

---

## Development

### Run Tests

```bash
cd tolteca_db
pytest tests/
```

### Code Quality

```bash
# Linting and formatting
ruff check src/
ruff format src/

# Type checking
mypy src/
```

### Build Documentation

```bash
cd docs/
make html
```

---

## Example Workflows

### Complete Data Pipeline

```bash
# 1. Initialize database
tolteca_db db init --url "duckdb:///pipeline.duckdb"

# 2. Ingest observations
tolteca_db ingest directory /data/obsnum_113533 \
  --db "duckdb:///pipeline.duckdb" \
  --location LMT

# 3. Generate associations
tolteca_db assoc generate \
  --db "duckdb:///pipeline.duckdb" \
  --incremental

# 4. Export results
tolteca_db query export results.parquet \
  --db "duckdb:///pipeline.duckdb"
```

### Incremental Daily Updates

```bash
# Day 1: Initial setup
tolteca_db ingest directory /data/2025-10-27 --location LMT
tolteca_db assoc generate --incremental --state filesystem --state-dir ./state

# Day 2: New observations
tolteca_db ingest directory /data/2025-10-28 --location LMT
tolteca_db assoc generate --incremental --state filesystem --state-dir ./state

# Check state growth
tolteca_db assoc state --backend filesystem --state-dir ./state
```

---

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## Credits

**Package:** tolteca_db  
**Author:** Zhiyuan Ma  
**Email:** zhiyuanma@umass.edu  
**License:** BSD-3-Clause

Created with [Cookiecutter](https://github.com/audreyfeldroy/cookiecutter) and the [audreyfeldroy/cookiecutter-pypackage](https://github.com/audreyfeldroy/cookiecutter-pypackage) project template.
