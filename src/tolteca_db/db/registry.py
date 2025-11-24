"""Registry table population utilities.

Populates DataProdType, DataKind, Flag, and Location registry tables with
standard TolTEC values. These tables must be populated before DataIngestor
can create DataProd entries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from tolteca_db.constants import DataProdAssocType as DataProdAssocTypeConst
from tolteca_db.constants import DataProdType as DataProdTypeConst
from tolteca_db.constants import FlagSeverity, ToltecDataKind
from tolteca_db.models.orm import (
    DataKind,
    DataProdAssocType,
    DataProdType,
    Flag,
    Location,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = ["populate_registry_tables"]


def populate_registry_tables(session: Session) -> dict[str, int]:
    """Populate registry tables with standard TolTEC values.
    
    Creates entries for:
    - DataProdType (8 types)
    - DataProdAssocType (7 association types)
    - DataKind (4 kinds from ToltecDataKind)
    - Flag (4 severity levels)
    - Location (1 default location: LMT)
    
    Safe to call multiple times (checks for existing entries).
    
    Parameters
    ----------
    session : Session
        Database session
    
    Returns
    -------
    dict[str, int]
        Counts of created entries: {"data_prod_type": N, "data_kind": M, ...}
    
    Examples
    --------
    >>> from tolteca_db.db import get_engine, get_session
    >>> engine = get_engine("duckdb:///tolteca.duckdb")
    >>> with Session(engine) as session:
    ...     counts = populate_registry_tables(session)
    ...     print(counts)
    {'data_prod_type': 8, 'data_prod_assoc_type': 7, 'data_kind': 4, 'flag': 4, 'location': 1}
    """
    counts = {
        "data_prod_type": 0,
        "data_prod_assoc_type": 0,
        "data_kind": 0,
        "flag": 0,
        "location": 0,
    }
    
    # Populate DataProdType
    for dp_type in DataProdTypeConst:
        stmt = select(DataProdType).where(DataProdType.label == dp_type.value)
        existing = session.scalar(stmt)
        if not existing:
            new_type = DataProdType(
                label=dp_type.value,
                description=f"TolTEC data product type: {dp_type.value}",
            )
            session.add(new_type)
            counts["data_prod_type"] += 1
    
    # Populate DataProdAssocType
    for assoc_type in DataProdAssocTypeConst:
        stmt = select(DataProdAssocType).where(
            DataProdAssocType.label == assoc_type.value
        )
        existing = session.scalar(stmt)
        if not existing:
            new_assoc_type = DataProdAssocType(
                label=assoc_type.value,
                description=f"TolTEC association type: {assoc_type.value}",
            )
            session.add(new_assoc_type)
            counts["data_prod_assoc_type"] += 1
    
    # Populate DataKind (individual flags from ToltecDataKind)
    data_kinds = [
        ("VnaSweep", "calibration", "Vector Network Analyzer sweep (bootstrapping)"),
        ("TargetSweep", "calibration", "Target sweep (refinement)"),
        ("Tune", "calibration", "Tune sweep (fine adjustment)"),
        ("RawTimeStream", "measurement", "Science timestream data"),
    ]
    for label, category, desc in data_kinds:
        stmt = select(DataKind).where(DataKind.label == label)
        existing = session.scalar(stmt)
        if not existing:
            new_kind = DataKind(
                label=label,
                category=category,
                description=desc,
            )
            session.add(new_kind)
            counts["data_kind"] += 1
    
    # Populate Flag severity levels
    for severity in FlagSeverity:
        stmt = select(Flag).where(
            (Flag.namespace == "severity") & 
            (Flag.label == severity.value.upper())
        )
        existing = session.scalar(stmt)
        if not existing:
            new_flag = Flag(
                namespace="severity",
                label=severity.value.upper(),
                description=f"Flag severity level: {severity.value}",
            )
            session.add(new_flag)
            counts["flag"] += 1
    
    # Populate Location (default LMT location)
    stmt = select(Location).where(Location.label == "LMT")
    existing = session.scalar(stmt)
    if not existing:
        new_location = Location(
            label="LMT",
            location_type="filesystem",
            root_uri="file:///data/lmt",
            priority=1,
            meta={"site": "Large Millimeter Telescope", "country": "Mexico"},
        )
        session.add(new_location)
        counts["location"] += 1
    
    session.commit()
    return counts
