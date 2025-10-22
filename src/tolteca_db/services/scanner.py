"""File scanner service for discovering data products."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from tolteca_db.io import get_registry
from tolteca_db.models.orm import Location
from tolteca_db.utils.filename import parse_toltec_filename

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class FileScanner:
    """
    Service for scanning directories and discovering data products.

    Scans storage locations registered in the database, identifies
    data product files, and optionally ingests them.

    Examples
    --------
    >>> scanner = FileScanner(session)
    >>> results = scanner.scan_location("LMT Data Archive")
    >>> print(f"Found {len(results['discovered'])} files")
    """

    def __init__(self, session: Session) -> None:
        """
        Initialize file scanner.

        Parameters
        ----------
        session : Session
            SQLAlchemy database session
        """
        self.session = session
        self.registry = get_registry()

    def scan_location(
        self,
        location_label: str,
        base_path: str | Path | None = None,
        recursive: bool = True,
        auto_ingest: bool = False,
    ) -> dict[str, Any]:
        """
        Scan a storage location for data products.

        Parameters
        ----------
        location_label : str
            Location label to scan
        base_path : str | Path | None, optional
            Base directory to scan. If None, uses location metadata, by default None
        recursive : bool, optional
            Whether to scan subdirectories, by default True
        auto_ingest : bool, optional
            Automatically ingest discovered files, by default False

        Returns
        -------
        dict[str, Any]
            Scan results with 'discovered', 'ingested', and 'errors' lists

        Raises
        ------
        ValueError
            If location not found or base_path not specified
        """
        # Find location
        stmt = select(Location).where(Location.label == location_label)
        location = self.session.scalar(stmt)

        if not location:
            msg = f"Location '{location_label}' not found in database"
            raise ValueError(msg)

        # Determine scan path
        if base_path:
            scan_path = Path(base_path)
        elif "base_path" in location.meta:
            scan_path = Path(location.meta["base_path"])
        else:
            msg = f"No base_path specified for location '{location_label}'"
            raise ValueError(msg)

        if not scan_path.exists():
            msg = f"Scan path does not exist: {scan_path}"
            raise ValueError(msg)

        # Collect results
        results = {
            "location": location_label,
            "scan_path": str(scan_path),
            "discovered": [],
            "ingested": [],
            "errors": [],
        }

        # Scan directory
        pattern = "**/*" if recursive else "*"
        for file_path in scan_path.glob(pattern):
            if not file_path.is_file():
                continue

            # Check if handler exists
            handler = self.registry.get_handler(file_path)
            if not handler:
                continue

            # Try to parse filename
            try:
                metadata = parse_toltec_filename(file_path.name)
                discovered_file = {
                    "path": str(file_path),
                    "format": handler.format_name,
                    "metadata": metadata,
                }
                results["discovered"].append(discovered_file)

                # Auto-ingest if requested
                if auto_ingest:
                    # Import here to avoid circular dependency
                    from tolteca_db.services.ingest import IngestService

                    service = IngestService(self.session)
                    ingest_result = service.ingest_file(
                        file_path, location_label, dry_run=False
                    )
                    results["ingested"].append(ingest_result)

            except ValueError as e:
                results["errors"].append(
                    {"path": str(file_path), "error": str(e)}
                )

        return results

    def scan_all_locations(
        self,
        recursive: bool = True,
        auto_ingest: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Scan all registered storage locations.

        Parameters
        ----------
        recursive : bool, optional
            Whether to scan subdirectories, by default True
        auto_ingest : bool, optional
            Automatically ingest discovered files, by default False

        Returns
        -------
        list[dict[str, Any]]
            List of scan results for each location
        """
        stmt = select(Location)
        locations = self.session.scalars(stmt).all()

        all_results = []
        for location in locations:
            if "base_path" not in location.meta:
                continue

            try:
                results = self.scan_location(
                    location.label,
                    recursive=recursive,
                    auto_ingest=auto_ingest,
                )
                all_results.append(results)
            except ValueError:
                # Skip locations with invalid paths
                continue

        return all_results

    def get_supported_formats(self) -> list[str]:
        """
        Get list of supported file formats.

        Returns
        -------
        list[str]
            Format names
        """
        return self.registry.list_formats()

    def get_supported_extensions(self) -> list[str]:
        """
        Get list of supported file extensions.

        Returns
        -------
        list[str]
            File extensions
        """
        return self.registry.list_extensions()
