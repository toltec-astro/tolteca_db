"""Database ingestor for TolTEC data files.

Populates database from scanned files, creating DataProd and DataProdSource entries.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from tolteca_db.constants import DataProdType as DataProdTypeConst, ToltecDataKind
from tolteca_db.models.metadata import RoachInterfaceMeta, RawObsMeta
from tolteca_db.models.orm import DataKind, DataProd, DataProdSource, DataProdType as DataProdTypeORM, Location

from .file_scanner import FileScanner, ParsedFileInfo, guess_info_from_file

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = ["DataIngestor", "IngestStats"]


@dataclass
class IngestStats:
    """Statistics for ingestion operation.
    
    Attributes
    ----------
    files_scanned : int
        Number of files scanned
    files_ingested : int
        Number of files successfully ingested
    files_skipped : int
        Number of files skipped (already exist)
    files_failed : int
        Number of files that failed
    data_prods_created : int
        Number of DataProd entries created
    sources_created : int
        Number of DataProdSource entries created
    """
    
    files_scanned: int = 0
    files_ingested: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    data_prods_created: int = 0
    sources_created: int = 0
    
    def __str__(self) -> str:
        return (
            f"Scanned: {self.files_scanned}, "
            f"Ingested: {self.files_ingested}, "
            f"Skipped: {self.files_skipped}, "
            f"Failed: {self.files_failed}, "
            f"DataProds: {self.data_prods_created}, "
            f"Sources: {self.sources_created}"
        )


class DataIngestor:
    """Database ingestor for TolTEC data files.
    
    Creates DataProd and DataProdSource entries from scanned files.
    
    Parameters
    ----------
    session : Session
        Database session
    location_pk : str
        Primary key of Location for file storage
    master : str, optional
        Master observation identifier, by default "toltec"
    nw_id : int, optional
        Network ID for raw observations, by default 0
    
    Examples
    --------
    >>> with Session(engine) as session:
    ...     ingestor = DataIngestor(session, location_pk="lmt")
    ...     stats = ingestor.ingest_directory("/data/toltec/raw")
    ...     print(stats)
    """
    
    def __init__(
        self,
        session: Session,
        location_pk: int | str,
        master: str = "toltec",
        nw_id: int = 0,
    ):
        self.session = session
        self.master = master
        self.nw_id = nw_id
        
        # Get location by pk or label
        if isinstance(location_pk, int):
            location = session.get(Location, location_pk)
        else:
            # Query by label
            from sqlalchemy import select
            stmt = select(Location).where(Location.label == location_pk)
            location = session.scalar(stmt)
        
        if location is None:
            msg = f"Location {location_pk!r} not found"
            raise ValueError(msg)
        
        self.location = location
        self.location_pk = location.pk
        self.location_root_path = self._parse_root_uri(location.root_uri)
        
        # Get dp_raw_obs type pk
        stmt = select(DataProdTypeORM).where(DataProdTypeORM.label == "dp_raw_obs")
        dp_type = session.scalar(stmt)
        if dp_type is None:
            msg = "DataProdType 'dp_raw_obs' not found - ensure registry is populated"
            raise ValueError(msg)
        self.dp_raw_obs_type_pk = dp_type.pk
    
    @staticmethod
    def _parse_root_uri(root_uri: str) -> Path:
        """Parse root URI to filesystem path.
        
        Parameters
        ----------
        root_uri : str
            Location root URI (e.g., 'file:///data/lmt')
        
        Returns
        -------
        Path
            Filesystem path
        """
        if root_uri.startswith('file://'):
            return Path(root_uri.replace('file://', '', 1))
        # For non-file URIs, assume they're already paths
        return Path(root_uri)
    
    def _make_relative_uri(self, file_path: Path) -> str:
        """Create source URI relative to location root.
        
        Parameters
        ----------
        file_path : Path
            Absolute file path
        
        Returns
        -------
        str
            Relative file path (without file:// prefix)
        """
        try:
            # Make path relative to location root
            # Use absolute() instead of resolve() to preserve symlinks
            rel_path = file_path.absolute().relative_to(self.location_root_path.absolute())
            return str(rel_path)
        except ValueError:
            # File is not under location root - use absolute path
            return str(file_path.absolute())
    
    def ingest_file(
        self,
        file_info: ParsedFileInfo,
        *,
        skip_existing: bool = True,
        obs_goal: str | None = None,
        source_name: str | None = None,
    ) -> tuple[DataProd | None, DataProdSource | None]:
        """Ingest a single file into the database.
        
        Parameters
        ----------
        file_info : ParsedFileInfo
            Parsed file information
        skip_existing : bool, optional
            Skip if source already exists, by default True
        obs_goal : str | None, optional
            Observation goal, by default None
        source_name : str | None, optional
            Source name, by default None
        
        Returns
        -------
        tuple[DataProd | None, DataProdSource | None]
            Created DataProd and DataProdSource, or (None, None) if skipped
        """
        import time
        if not hasattr(self, '_timings'):
            self._timings = {
                'make_relative_uri': 0,
                'check_existing': 0,
                'file_exists': 0,
                'get_or_create_raw_obs': 0,
                'create_source': 0,
            }
        
        # Build source URI relative to location root
        t0 = time.time()
        source_uri = self._make_relative_uri(file_info.filepath)
        self._timings['make_relative_uri'] += time.time() - t0
        
        # Check if source already exists
        if skip_existing:
            t0 = time.time()
            stmt = select(DataProdSource).where(DataProdSource.source_uri == source_uri)
            existing = self.session.scalar(stmt)
            self._timings['check_existing'] += time.time() - t0
            if existing is not None:
                return None, None
        
        # Check if file exists
        t0 = time.time()
        file_exists = file_info.filepath.exists()
        self._timings['file_exists'] += time.time() - t0
        
        # Get or create raw observation DataProd
        t0 = time.time()
        data_prod = self._get_or_create_raw_obs(
            file_info,
            obs_goal=obs_goal,
            source_name=source_name,
        )
        self._timings['get_or_create_raw_obs'] += time.time() - t0
        
        # Create DataProdSource
        t0 = time.time()
        source = self._create_source(file_info, data_prod.pk, source_uri, file_exists=file_exists)
        self._timings['create_source'] += time.time() - t0
        
        return data_prod, source
    
    def ingest_directory(
        self,
        root_path: str | Path,
        *,
        pattern: str = "*.nc",
        recursive: bool = True,
        skip_existing: bool = True,
        commit_interval: int = 100,
    ) -> IngestStats:
        """Ingest all files in directory.
        
        Parameters
        ----------
        root_path : str | Path
            Root directory to scan
        pattern : str, optional
            Glob pattern for files, by default "*.nc"
        recursive : bool, optional
            Scan recursively, by default True
        skip_existing : bool, optional
            Skip existing files, by default True
        commit_interval : int, optional
            Commit every N files, by default 100
        
        Returns
        -------
        IngestStats
            Statistics for ingestion operation
        """
        scanner = FileScanner(root_path, recursive=recursive, pattern=pattern)
        stats = IngestStats()
        
        for file_info in scanner.scan():
            stats.files_scanned += 1
            
            try:
                data_prod, source = self.ingest_file(
                    file_info,
                    skip_existing=skip_existing,
                )
                
                if data_prod is None:
                    stats.files_skipped += 1
                else:
                    stats.files_ingested += 1
                    stats.data_prods_created += 1 if data_prod else 0
                    stats.sources_created += 1 if source else 0
                
                # Periodic commit
                if stats.files_scanned % commit_interval == 0:
                    self.session.commit()
            
            except Exception as e:
                stats.files_failed += 1
                self.session.rollback()  # Rollback failed transaction
                print(f"Failed to ingest {file_info.filepath}: {e}")
                continue
        
        # Final commit
        self.session.commit()
        
        return stats
    
    def _get_or_create_raw_obs(
        self,
        file_info: ParsedFileInfo,
        obs_goal: str | None = None,
        source_name: str | None = None,
    ) -> DataProd:
        """Get or create raw observation DataProd.
        
        Creates a single DataProd per (master, obsnum, subobsnum, scannum) combination.
        Multiple interface files link to the same DataProd.
        """
        # Check if already exists by querying metadata
        stmt = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == self.dp_raw_obs_type_pk)
            .where(DataProd.meta['master'].as_string() == self.master)
            .where(DataProd.meta['obsnum'].as_integer() == file_info.obsnum)
            .where(DataProd.meta['subobsnum'].as_integer() == file_info.subobsnum)
            .where(DataProd.meta['scannum'].as_integer() == file_info.scannum)
        )
        existing = self.session.scalar(stmt)
        if existing is not None:
            return existing
        
        # Create RawObsMeta
        raw_obs_meta = RawObsMeta(
            name=f"raw_{self.master}_{file_info.obsnum}_{file_info.subobsnum}_{file_info.scannum}",
            data_prod_type=DataProdTypeConst.DP_RAW_OBS,
            tag="raw_obs",
            master=self.master,
            obsnum=file_info.obsnum,
            subobsnum=file_info.subobsnum,
            scannum=file_info.scannum,
            data_kind=file_info.data_kind.value if file_info.data_kind else 0,
            obs_goal=obs_goal,
            source_name=source_name,
            obs_datetime=file_info.obs_datetime,
        )
        
        # Create DataProd (pk is auto-generated)
        data_prod = DataProd(
            data_prod_type_fk=self.dp_raw_obs_type_pk,
            meta=raw_obs_meta,
        )
        
        self.session.add(data_prod)
        self.session.flush()  # Get auto-assigned pk
        
        # Link to DataKind if known (skip for now - data_kind stored in meta as int)
        # if file_info.data_kind:
        #     self._link_data_kind(data_prod, file_info.data_kind)
        
        return data_prod
    
    def _create_source(
        self,
        file_info: ParsedFileInfo,
        data_prod_pk: str,
        source_uri: str,
        file_exists: bool = True,
    ) -> DataProdSource:
        """Create DataProdSource entry.
        
        Parameters
        ----------
        file_info : ParsedFileInfo
            Parsed file information
        data_prod_pk : str
            Data product primary key
        source_uri : str
            Source URI for the file
        file_exists : bool, optional
            Whether the physical file exists, by default True
        
        Returns
        -------
        DataProdSource
            Created source entry
        """
        # Calculate file metadata if file exists
        if file_exists:
            import time
            t0 = time.time()
            file_size = file_info.filepath.stat().st_size
            self._timings['stat'] = self._timings.get('stat', 0) + (time.time() - t0)
            availability_state = "available"
        else:
            file_size = None
            availability_state = "missing"
        
        # Create RoachInterfaceMeta
        interface_meta = RoachInterfaceMeta(
            obsnum=file_info.obsnum,
            subobsnum=file_info.subobsnum,
            scannum=file_info.scannum,
            master=self.master,
            roach=file_info.roach,
            nw_id=self.nw_id,
            interface=file_info.interface,
            data_kind=file_info.data_kind.value if file_info.data_kind else None,
        )
        
        # Create DataProdSource
        source = DataProdSource(
            source_uri=source_uri,
            location_fk=self.location_pk,
            data_prod_fk=data_prod_pk,
            checksum=None,
            size=file_size,
            availability_state=availability_state,
            meta=interface_meta,
        )
        
        self.session.add(source)
        
        return source
    
    def _link_data_kind(self, data_prod: DataProd, data_kind: ToltecDataKind) -> None:
        """Link DataProd to DataKind."""
        # Get DataKind entry
        stmt = select(DataKind).where(DataKind.label == data_kind.value)
        dk = self.session.scalar(stmt)
        
        if dk is not None and dk not in data_prod.data_kinds:
            data_prod.data_kinds.append(dk)
