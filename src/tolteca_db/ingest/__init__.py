"""Ingestion module for TolTEC data files.

Scans directories, parses filenames, and populates database with file metadata.
"""

from __future__ import annotations

from .file_scanner import FileScanner, ParsedFileInfo, guess_info_from_file
from .ingest import DataIngestor, IngestStats

__all__ = [
    "FileScanner",
    "ParsedFileInfo",
    "guess_info_from_file",
    "DataIngestor",
    "IngestStats",
]
