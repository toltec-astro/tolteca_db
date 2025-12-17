"""Ingestor for LMT telescope metadata from CSV.

Creates DataProd entries (if needed) and DataProdSource entries with TelInterfaceMeta.
Updates RawObsMeta with denormalized telescope fields for efficient querying.

The CSV now includes FileName and Valid columns, making it structurally similar to toltec_db:
- Both provide ObsNum/SubObsNum/ScanNum quartet identification
- Both include filename for file path construction
- Both have validation status
- toltec_db: roach interface metadata
- lmtmc CSV: tel interface metadata

This unified structure allows similar ingestion patterns for both sources.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from tolteca_db.constants import ToltecDataKind
from tolteca_db.models.metadata import RawObsMeta
from tolteca_db.models.orm import DataProd, DataProdSource, Location

from .tel_csv_parser import parse_tel_csv, TelCSVRow

__all__ = ["TelCSVIngestor", "TelIngestStats"]


@dataclass
class TelIngestStats:
    """Statistics for tel CSV ingestion.
    
    Attributes
    ----------
    rows_scanned : int
        Number of CSV rows processed
    rows_ingested : int
        Number of rows successfully ingested
    rows_skipped : int
        Number of rows skipped (already exists)
    rows_failed : int
        Number of rows that failed
    data_prods_created : int
        Number of DataProd entries created
    data_prods_updated : int
        Number of existing DataProd.meta entries updated with tel fields
    sources_created : int
        Number of DataProdSource entries created
    """
    
    rows_scanned: int = 0
    rows_ingested: int = 0
    rows_skipped: int = 0
    rows_failed: int = 0
    data_prods_created: int = 0
    data_prods_updated: int = 0
    sources_created: int = 0
    
    def __str__(self) -> str:
        return (
            f"Scanned: {self.rows_scanned}, "
            f"Ingested: {self.rows_ingested}, "
            f"Skipped: {self.rows_skipped}, "
            f"Failed: {self.rows_failed}, "
            f"DataProds Created: {self.data_prods_created}, "
            f"DataProds Updated: {self.data_prods_updated}, "
            f"Sources: {self.sources_created}"
        )


class TelCSVIngestor:
    """Ingest LMT telescope metadata from CSV into tolteca_db.
    
    Creates DataProd entries (if they don't exist) and DataProdSource entries
    with tel_toltec interface metadata. Updates RawObsMeta with denormalized
    telescope fields for efficient querying.
    
    Tel metadata is a first-class data source - DataProd entries can be created
    from tel metadata alone, independent of roach interface ingestion.
    
    Parameters
    ----------
    session : Session
        Database session
    location_pk : int | str
        Location PK for tel metadata (e.g., "LMT")
    skip_existing : bool, default=True
        Skip if tel source already exists for this obs
    create_data_prods : bool, default=True
        Create DataProd entries if they don't exist
    commit_batch_size : int, default=100
        Commit every N sources for progress tracking
    
    Examples
    --------
    >>> with Session(engine) as session:
    ...     location = session.query(Location).filter_by(label="LMT").first()
    ...     ingestor = TelCSVIngestor(session, location.pk)
    ...     stats = ingestor.ingest_csv("run/lmtmc_toltec_metadata.csv")
    ...     print(stats)
    """
    
    def __init__(
        self,
        session: Session,
        location_pk: int | str,
        skip_existing: bool = True,
        create_data_prods: bool = True,
        commit_batch_size: int = 100,
    ):
        self.session = session
        self.location_pk = location_pk
        self.skip_existing = skip_existing
        self.create_data_prods = create_data_prods
        self.commit_batch_size = commit_batch_size
    
    def ingest_csv(self, csv_path: Path | str) -> TelIngestStats:
        """Ingest tel metadata from CSV file.
        
        Parameters
        ----------
        csv_path : Path | str
            Path to lmtmc_toltec_metadata.csv
        
        Returns
        -------
        TelIngestStats
            Ingestion statistics
        
        Notes
        -----
        For each CSV row:
        1. Find or create DataProd (dp_raw_obs with matching quartet)
        2. Create DataProdSource with TelInterfaceMeta
        3. Update DataProd.meta (RawObsMeta) with denormalized tel fields
        
        Denormalized fields enable efficient queries:
        - WHERE meta['tau'] < 0.1 (direct, no JOIN)
        vs
        - JOIN data_prod_source WHERE src.meta['tau'] < 0.1 (expensive)
        """
        stats = TelIngestStats()
        
        for row in parse_tel_csv(csv_path):
            stats.rows_scanned += 1
            
            try:
                # Find matching DataProd (dp_raw_obs with matching quartet)
                stmt = (
                    select(DataProd)
                    .where(DataProd.data_prod_type_fk == 1)  # dp_raw_obs
                    .where(DataProd.meta["obsnum"].as_integer() == row.obsnum)
                    .where(DataProd.meta["subobsnum"].as_integer() == row.subobsnum)
                    .where(DataProd.meta["scannum"].as_integer() == row.scannum)
                    .where(DataProd.meta["master"].as_string() == "tcs")
                )
                data_prod = self.session.execute(stmt).scalar_one_or_none()
                
                # Create DataProd if it doesn't exist
                if data_prod is None:
                    if not self.create_data_prods:
                        stats.rows_skipped += 1
                        continue
                    
                    # Create new DataProd with tel metadata
                    tel_meta = row.tel_metadata
                    data_prod = DataProd(
                        data_prod_type_fk=1,  # dp_raw_obs
                        meta=RawObsMeta(
                            name=f"tcs-{row.obsnum}-{row.subobsnum}-{row.scannum}",
                            data_prod_type="dp_raw_obs",
                            # ObsIdMixin fields
                            obsnum=row.obsnum,
                            subobsnum=row.subobsnum,
                            scannum=row.scannum,
                            master="tcs",
                            # TelMetaMixin fields (denormalized)
                            obs_datetime=tel_meta.obs_datetime,
                            source_name=tel_meta.source_name,
                            obs_goal=tel_meta.obs_goal,
                            project_id=tel_meta.project_id,
                            obs_pgm=tel_meta.obs_pgm,
                            integration_time=tel_meta.integration_time,
                            az_deg=tel_meta.az_deg,
                            el_deg=tel_meta.el_deg,
                            user_az_offset_arcsec=tel_meta.user_az_offset_arcsec,
                            user_el_offset_arcsec=tel_meta.user_el_offset_arcsec,
                            paddle_az_offset_arcsec=tel_meta.paddle_az_offset_arcsec,
                            paddle_el_offset_arcsec=tel_meta.paddle_el_offset_arcsec,
                            m1_zernike=tel_meta.m1_zernike,
                            m2_offset_mm=tel_meta.m2_offset_mm,
                            tau=tel_meta.tau,
                            crane_in_beam=tel_meta.crane_in_beam,
                            # RawObsMeta-specific: tel_toltec interface = LmtTel data_kind
                            data_kind=ToltecDataKind.LmtTel.value,
                        ),
                    )
                    self.session.add(data_prod)
                    self.session.flush()  # Get pk for foreign key
                    stats.data_prods_created += 1
                else:
                    # Update existing DataProd with tel fields
                    tel_meta = row.tel_metadata
                    data_prod.meta.obs_datetime = tel_meta.obs_datetime
                    data_prod.meta.source_name = tel_meta.source_name
                    data_prod.meta.obs_goal = tel_meta.obs_goal
                    data_prod.meta.project_id = tel_meta.project_id
                    data_prod.meta.obs_pgm = tel_meta.obs_pgm
                    data_prod.meta.integration_time = tel_meta.integration_time
                    data_prod.meta.az_deg = tel_meta.az_deg
                    data_prod.meta.el_deg = tel_meta.el_deg
                    data_prod.meta.user_az_offset_arcsec = tel_meta.user_az_offset_arcsec
                    data_prod.meta.user_el_offset_arcsec = tel_meta.user_el_offset_arcsec
                    data_prod.meta.paddle_az_offset_arcsec = tel_meta.paddle_az_offset_arcsec
                    data_prod.meta.paddle_el_offset_arcsec = tel_meta.paddle_el_offset_arcsec
                    data_prod.meta.m1_zernike = tel_meta.m1_zernike
                    data_prod.meta.m2_offset_mm = tel_meta.m2_offset_mm
                    data_prod.meta.tau = tel_meta.tau
                    data_prod.meta.crane_in_beam = tel_meta.crane_in_beam
                    
                    # UNION data_kind: combine existing data_kind with LmtTel flag
                    # Example: Timestream (4) | LmtTel (16) = 20
                    existing_data_kind = data_prod.meta.data_kind or 0
                    data_prod.meta.data_kind = existing_data_kind | ToltecDataKind.LmtTel.value
                    
                    # Mark meta as modified so SQLAlchemy detects the change
                    flag_modified(data_prod, 'meta')
                    
                    stats.data_prods_updated += 1
                
                # Check if tel source already exists (check by filename-based URI)
                # Filename from CSV: /data_lmt/tel/tel_toltec_2022-01-14_093026_00_0001.nc
                # We want relative path from location root: tel/tel_toltec_*.nc
                from pathlib import Path
                filename_path = Path(row.filename)
                
                # Extract relative path from /data_lmt/ onwards
                # Example: /data_lmt/tel/file.nc → tel/file.nc
                try:
                    # Find 'data_lmt' in path and take everything after it
                    parts = filename_path.parts
                    if 'data_lmt' in parts:
                        data_lmt_idx = parts.index('data_lmt')
                        rel_parts = parts[data_lmt_idx + 1:]  # Skip 'data_lmt' itself
                        source_uri = str(Path(*rel_parts))
                    else:
                        # Fallback: use filename as-is
                        source_uri = row.filename
                except (ValueError, IndexError):
                    source_uri = row.filename
                
                if self.skip_existing:
                    stmt = select(DataProdSource).where(
                        DataProdSource.source_uri == source_uri
                    )
                    existing = self.session.execute(stmt).scalar_one_or_none()
                    if existing:
                        stats.rows_skipped += 1
                        continue
                
                # Create DataProdSource for tel file
                # Note: We don't check if file actually exists - availability_state will be "UNKNOWN"
                # until verified. This allows ingesting metadata even if files are offline.
                source = DataProdSource(
                    source_uri=source_uri,
                    data_prod_fk=data_prod.pk,
                    location_fk=self.location_pk,
                    role="METADATA",
                    availability_state="UNKNOWN",  # Will be verified later
                    meta=row.tel_metadata,  # Type-safe TelInterfaceMeta storage
                )
                
                self.session.add(source)
                stats.sources_created += 1
                stats.rows_ingested += 1
                
                # Commit in batches
                if stats.sources_created % self.commit_batch_size == 0:
                    self.session.commit()
            
            except Exception as e:
                print(f"Failed to ingest row (obsnum={row.obsnum}): {e}")
                stats.rows_failed += 1
                self.session.rollback()
                continue
        
        # Final commit
        self.session.commit()
        
        return stats
