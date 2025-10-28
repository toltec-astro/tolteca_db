"""Tests for optional production enhancements.

Tests verify:
1. Eager loading methods in DataProductRepository
2. Structured metadata with adaptix dataclasses (Phase 3)
3. Composite index creation (verified at schema level)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel

from tolteca_db.constants import DataProdType, ProductKind
from tolteca_db.db.repository import DataProductRepository
from tolteca_db.models.metadata import DataProdMetaBase, RawObsMeta
from tolteca_db.models.orm import Base, DataProd, DataProdSource, Location


def utc_now_iso() -> str:
    """Return current UTC time as ISO 8601 string (Python 3.13 compatible)."""
    return datetime.now(tz=timezone.utc).isoformat()


@pytest.fixture
def in_memory_db():
    """Create in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    # Properly dispose engine to prevent unclosed connection warnings
    engine.dispose()


@pytest.fixture
def session(in_memory_db):
    """Create database session."""
    with Session(in_memory_db) as session:
        yield session


@pytest.fixture
def sample_product(session):
    """Create sample product with storage locations."""
    # Create data_prod_type first
    from tolteca_db.models.orm import DataProdType
    data_prod_type = DataProdType(
        pk=1,
        label="dp_raw_obs",
        description="Raw observation",
        level=0
    )
    session.add(data_prod_type)
    
    # Create location
    from tolteca_db.models.orm import Location
    location = Location(
        pk=1,
        label="Test Location",
        location_type="filesystem",
        root_uri="file:///data",
        priority=1,
        meta={}
    )
    session.add(location)
    
    # Create product with structured metadata
    meta = RawObsMeta(
        tag="raw_obs",  # Discriminator
        name="test_obs_001",
        data_prod_type="dp_raw_obs",  # String value
        master="toltec",
        obsnum=12345,
        subobsnum=0,
        scannum=1,
        data_kind=1
    )
    product = DataProd(
        pk=1,
        data_prod_type_fk=1,
        lifecycle_status="ACTIVE",
        meta=meta
    )
    session.add(product)
    
    # Create storage
    from tolteca_db.models.orm import DataProdSource
    storage = DataProdSource(
        data_prod_fk=1,
        location_fk=1,
        source_uri="file:///data/test.fits",
        role="PRIMARY",
        size=1024
    )
    session.add(storage)
    
    session.commit()
    return product.pk


class TestEagerLoading:
    """Test eager loading methods for N+1 query prevention."""

    def test_get_with_storage(self, session, sample_product):
        """Test getting product with storage eagerly loaded."""
        repo = DataProductRepository(session, DataProd)
        product = repo.get_with_storage(sample_product)
        
        assert product is not None
        assert product.pk == sample_product
        # Access sources - should not trigger additional query
        assert len(product.sources) == 1
        assert product.sources[0].source_uri == "file:///data/test.fits"

    def test_get_with_flags(self, session, sample_product):
        """Test getting product with flags eagerly loaded."""
        repo = DataProductRepository(session, DataProd)
        product = repo.get_with_flags(sample_product)
        
        assert product is not None
        assert product.pk == sample_product
        # Access flags - should not trigger additional query
        assert len(product.flag) == 0  # No flags added in fixture

    def test_get_with_all_relations(self, session, sample_product):
        """Test getting product with all relationships eagerly loaded."""
        repo = DataProductRepository(session, DataProd)
        product = repo.get_with_all_relations(sample_product)
        
        assert product is not None
        assert len(product.sources) == 1
        assert len(product.flags) == 0

    def test_list_with_storage(self, session, sample_product):
        """Test listing products with storage eagerly loaded."""
        repo = DataProductRepository(session, DataProd)
        products = repo.list_with_storage(data_prod_type_fk=1, limit=10)
        
        assert len(products) == 1
        assert products[0].pk == sample_product
        # Access storage without N+1 queries
        assert len(products[0].sources) == 1

    def test_list_with_storage_no_filter(self, session, sample_product):
        """Test listing all products with storage."""
        repo = DataProductRepository(session, DataProd)
        products = repo.list_with_storage()
        
        assert len(products) == 1
        assert products[0].sources[0].location_fk == "test_loc"


class TestStructuredMetadata:
    """Test structured metadata with adaptix dataclasses (Phase 3)."""

    def test_dataclass_instantiation(self):
        """Test that metadata dataclasses can be instantiated."""
        meta = RawObsMeta(
            tag="raw_obs",  # Discriminator
            name="test_obs",
            data_prod_type="dp_raw_obs",
            master="toltec",
            obsnum=12345,
            subobsnum=0,
            scannum=1,
            data_kind=1
        )
        
        assert meta.name == "test_obs"
        assert meta.obsnum == 12345
        assert meta.master == "toltec"

    def test_dataclass_field_access(self):
        """Test that dataclass fields provide IDE autocomplete."""
        meta = RawObsMeta(
            tag="raw_obs",  # Discriminator
            name="test",
            data_prod_type="dp_raw_obs",
            master="toltec",
            obsnum=1,
            subobsnum=0,
            scannum=0,
            data_kind=1
        )
        
        # Type-safe field access (IDE autocomplete works)
        assert meta.obsnum == 1
        assert meta.nw_id is None
        assert meta.obs_goal is None

    def test_dataclass_with_product(self, session):
        """Test structured metadata with AdaptixJSON union types (Literal discriminators)."""
        # Create data_prod_type
        from tolteca_db.models.orm import DataProdType
        data_prod_type = DataProdType(
            pk=99,
            label="dp_raw_obs",
            description="Raw observation",
            level=0
        )
        session.add(data_prod_type)
        session.commit()

        # Create structured metadata as dataclass
        # NOTE: tag field is the Literal discriminator for union types!
        meta = RawObsMeta(
            tag="raw_obs",  # Discriminator - identifies this as RawObsMeta
            name="test_structured",
            data_prod_type="dp_raw_obs",
            master="ics",
            obsnum=99999,
            subobsnum=1,
            scannum=2,
            data_kind=4,
            obs_goal="science",
            source_name="NGC1234"
        )

        # AdaptixJSON with union types - pass dataclass directly!
        product = DataProd(
            pk=999,
            data_prod_type_fk=99,
            lifecycle_status="ACTIVE",
            meta=meta  # Direct assignment - AdaptixJSON handles union discrimination
        )

        session.add(product)
        session.commit()

        # Retrieve - AdaptixJSON deserializes to correct type based on 'tag' field!
        retrieved = session.get(DataProd, 999)
        assert retrieved is not None

        # Type-safe access - no manual conversion needed!
        assert isinstance(retrieved.meta, RawObsMeta)
        assert retrieved.meta.obsnum == 99999
        assert retrieved.meta.obs_goal == "science"
        assert retrieved.meta.source_name == "NGC1234"        # JSON operators still work for queries!
        # session.query(DataProd).filter(DataProd.meta['obsnum'] == 99999)


class TestCompositeIndex:
    """Test composite index creation (schema-level verification)."""

    def test_composite_index_exists(self, in_memory_db):
        """Verify composite index exists in schema."""
        # Get table metadata
        inspector = create_engine("sqlite:///:memory:").pool._creator
        metadata = Base.metadata
        data_prod_table = metadata.tables["data_prod"]
        
        # Check for composite index
        index_names = [idx.name for idx in data_prod_table.indexes]
        
        # Verify our new composite index
        assert "ix_base_type_availability" in index_names
        
        # Verify index columns
        composite_index = next(
            idx for idx in data_prod_table.indexes
            if idx.name == "ix_base_type_availability"
        )
        column_names = [col.name for col in composite_index.columns]
        assert column_names == ["base_type", "availability_state"]

    def test_existing_indexes_preserved(self, in_memory_db):
        """Verify existing indexes are still present."""
        metadata = Base.metadata
        data_prod_table = metadata.tables["data_prod"]
        index_names = [idx.name for idx in data_prod_table.indexes]
        
        # Original composite index should still exist
        assert "ix_product_kind_status" in index_names


class TestAdaptixJSONIntegration:
    """Test AdaptixJSON integration for type-safe JSON storage (Section 7)."""

    def test_interface_file_meta_automatic_serialization(self, session):
        """
        Demonstrate AdaptixJSON Section 7 benefit: automatic type-safe serialization.
        
        Key advantage: Store/retrieve dataclasses without manual conversion.
        """
        from tolteca_db.models.metadata import InterfaceFileMeta
        
        # Create location using NEW Location model (from orm/source.py)
        # NOTE: pk is autoincrement int, so we don't specify it
        location = Location(
            label="primary",
            location_type="filesystem",
            root_uri="file:///data",
            priority=1,
        )
        session.add(location)
        session.flush()  # Get the auto-generated pk
        loc_pk = location.pk
        
        # Create InterfaceFileMeta dataclass
        interface_meta = InterfaceFileMeta(
            interface_id="toltec_roach0",
            roach=0,
            file_suffix="0001",
            file_ext=".nc",
            meta={"additional": "info"},
        )
        
        # ✅ AdaptixJSON: Direct dataclass assignment - NO manual conversion!
        # DataProdSource uses composite PK (data_prod_fk, location_fk)
        source = DataProdSource(
            data_prod_fk=999,  # Can use any value for test
            location_fk=loc_pk,  # Use auto-generated location pk
            source_uri="/data/file.nc",
            meta=interface_meta,  # Direct assignment!
        )
        
        session.add(source)
        session.commit()
        
        # ✅ AdaptixJSON: Automatic deserialization to dataclass!
        # Query with composite key
        from sqlmodel import select
        
        retrieved = session.exec(
            select(DataProdSource).where(
                DataProdSource.data_prod_fk == 999,
                DataProdSource.location_fk == loc_pk
            )
        ).first()
        assert retrieved is not None
        
        # Type-safe access - no manual conversion needed
        assert isinstance(retrieved.meta, InterfaceFileMeta)
        assert retrieved.meta.interface_id == "toltec_roach0"
        assert retrieved.meta.roach == 0
        assert retrieved.meta.file_ext == ".nc"
        
        # Compare to plain JSON approach which would require:
        # ❌ Without AdaptixJSON:
        #    meta_dict = retrieved.meta  # Just a dict
        #    meta_obj = metadata_from_dict(meta_dict, InterfaceFileMeta)
        #    assert meta_obj.interface == "toltec"

    def test_interface_file_meta_nullable(self, session):
        """Test AdaptixJSON handles None values correctly."""
        from tolteca_db.models.metadata import InterfaceFileMeta
        
        location = Location(
            label="test",
            location_type="filesystem",
            root_uri="file:///test",
            priority=1,
        )
        session.add(location)
        session.flush()
        loc_pk = location.pk
        
        # Create source WITHOUT metadata
        source = DataProdSource(
            data_prod_fk=998,
            location_fk=loc_pk,
            source_uri="/test/file.nc",
            meta=None,  # No metadata
        )
        
        session.add(source)
        session.commit()
        
        # Retrieve - meta should be None
        from sqlmodel import select
        
        retrieved = session.exec(
            select(DataProdSource).where(
                DataProdSource.data_prod_fk == 998,
                DataProdSource.location_fk == loc_pk
            )
        ).first()
        assert retrieved is not None
        assert retrieved.meta is None

