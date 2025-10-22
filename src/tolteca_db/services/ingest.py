"""Ingest service for data product ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from tolteca_db.models.orm import DataProduct, DataProductStorage, Location
from tolteca_db.models.schemas import DataProductCreate, DataProductStorageCreate
from tolteca_db.utils.filename import parse_toltec_filename
from tolteca_db.utils.hash import product_id_hash

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class IngestService:
    """
    Service for ingesting data product files.

    Coordinates filename parsing, validation, and database record creation
    for TolTEC data products.
    """

    def __init__(self, session: Session) -> None:
        """
        Initialize ingest service.

        Parameters
        ----------
        session : Session
            SQLAlchemy database session
        """
        self.session = session

    def ingest_file(
        self, file_path: Path, location_label: str, dry_run: bool = False
    ) -> dict[str, Any]:
        """
        Ingest a data product file.

        Parameters
        ----------
        file_path : Path
            Path to the data file
        location_label : str
            Label of the storage location
        dry_run : bool, optional
            If True, validate only without creating records, by default False

        Returns
        -------
        dict[str, Any]
            Dictionary with ingestion results including product_pk, base_type,
            metadata, and location

        Raises
        ------
        ValueError
            If filename parsing fails or location not found
        """
        # Parse filename to extract metadata
        metadata = parse_toltec_filename(file_path.name)

        # Find or validate location
        stmt = select(Location).where(Location.label == location_label)
        location = self.session.scalar(stmt)

        if not location:
            msg = f"Location '{location_label}' not found in database"
            raise ValueError(msg)

        # Extract identity components for product_pk generation
        base_type = metadata["base_type"]
        identity = {
            "obsnum": metadata.get("obsnum"),
            "subobsnum": metadata.get("subobsnum"),
            "scannum": metadata.get("scannum"),
        }

        # Generate product primary key
        product_pk = product_id_hash(base_type, identity)

        if dry_run:
            return {
                "product_pk": product_pk,
                "base_type": base_type,
                "metadata": metadata,
                "location": location_label,
            }

        # Check if product already exists
        stmt = select(DataProduct).where(DataProduct.product_pk == product_pk)
        existing = self.session.scalar(stmt)

        if existing:
            # Update existing product
            product = existing
            product.status = metadata.get("status", "RAW")
        else:
            # Create new product
            product_data = DataProductCreate(
                base_type=base_type,
                status=metadata.get("status", "RAW"),
                meta=metadata,
            )

            product = DataProduct(
                product_pk=product_pk, **product_data.model_dump()
            )
            self.session.add(product)

        # Create storage record
        storage_data = DataProductStorageCreate(
            product_fk=product_pk,
            location_fk=location.location_pk,
            storage_key=str(file_path),
            size=file_path.stat().st_size if file_path.exists() else 0,
        )

        storage = DataProductStorage(**storage_data.model_dump())
        self.session.add(storage)

        # Commit transaction
        self.session.commit()

        return {
            "product_pk": product_pk,
            "base_type": base_type,
            "metadata": metadata,
            "location": location_label,
        }
