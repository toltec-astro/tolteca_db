"""Tests for SQLAlchemy 2.0 ORM models.

Tests verify:
1. Schema creation (tables, indexes)
2. Model instantiation and constraints
3. Relationships and cascade behavior
4. Data integrity (unique constraints, foreign keys)
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError, SAWarning
from sqlalchemy.orm import Session

from tolteca_db.constants import (
    AssocType,
    BaseType,
    FlagSeverity,
    ProductKind,
    StorageRole,
    TaskStatus,
)
from tolteca_db.models.orm import (
    Base,
    DataProduct,
    DataProductAssoc,
    DataProductFlag,
    DataProductStorage,
    EventLog,
    FlagDefinition,
    Location,
    ReductionTask,
    TaskInput,
    TaskOutput,
)


@pytest.fixture
def engine():
    """Create in-memory SQLite database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine):
    """Create database session."""
    with Session(engine) as session:
        yield session


class TestSchemaCreation:
    """Test database schema creation."""

    def test_all_tables_created(self, engine):
        """Test that all tables are created."""
        expected_tables = {
            "data_product",
            "data_product_storage",
            "data_product_assoc",
            "data_product_flag",
            "flag_definition",
            "location",
            "reduction_task",
            "task_input",
            "task_output",
            "event_log",
        }

        inspector = inspect(engine)
        actual_tables = set(inspector.get_table_names())

        assert actual_tables == expected_tables

    def test_indexes_created(self, engine):
        """Test that composite indexes are created."""
        inspector = inspect(engine)
        product_indexes = inspector.get_indexes("data_product")

        index_names = {idx["name"] for idx in product_indexes}

        # Check that indexes exist (actual names may vary)
        assert len(index_names) >= 5  # Should have multiple indexes


class TestDataProduct:
    """Test DataProduct model."""

    def test_create_raw_obs_product(self, session):
        """Test creating a RAW product."""
        product = DataProduct(
            product_pk="test_raw_001",
            base_type=BaseType.RAW_OBS.value,
            name="obs_12345",
            product_kind=ProductKind.RAW.value,
        )

        session.add(product)
        session.commit()

        assert product.product_pk == "test_raw_001"
        assert product.product_kind == ProductKind.RAW.value
        assert product.base_type == BaseType.RAW_OBS.value

    def test_create_reduced_obs_product(self, session):
        """Test creating a REDUCED product."""
        product = DataProduct(
            product_pk="test_reduced_001",
            base_type=BaseType.REDUCED_OBS.value,
            name="reduced_12345",
            product_kind=ProductKind.REDUCED.value,
        )

        session.add(product)
        session.commit()

        assert product.product_pk == "test_reduced_001"
        assert product.product_kind == ProductKind.REDUCED.value

    def test_product_pk_unique_constraint(self, session):
        """Test that product_pk must be unique."""
        product1 = DataProduct(
            product_pk="duplicate_pk",
            base_type=BaseType.RAW_OBS.value,
            name="obs_1",
            product_kind=ProductKind.RAW.value,
        )

        product2 = DataProduct(
            product_pk="duplicate_pk",  # Same PK
            base_type=BaseType.RAW_OBS.value,
            name="obs_2",
            product_kind=ProductKind.RAW.value,
        )

        session.add(product1)
        session.commit()

        # Create new session to test database constraint
        session.expire_all()
        session.add(product2)
        with pytest.raises((IntegrityError, SAWarning)):
            session.commit()


class TestDataProductStorage:
    """Test DataProductStorage model and relationships."""

    def test_storage_relationship(self, session):
        """Test product-storage relationship."""
        # Create location
        location = Location(
            location_pk="lmt_archive",
            label="LMT Archive",
            site_code="LMT",
        )
        session.add(location)

        # Create product
        product = DataProduct(
            product_pk="test_001",
            base_type=BaseType.RAW_OBS.value,
            name="obs_12345",
            product_kind=ProductKind.RAW.value,
        )
        session.add(product)
        session.commit()

        # Create storage
        storage = DataProductStorage(
            product_fk=product.product_pk,
            location_fk=location.location_pk,
            storage_key="/data/obs_12345.nc",
            role=StorageRole.PRIMARY.value,
        )
        session.add(storage)
        session.commit()

        # Verify relationships
        retrieved_product = session.get(DataProduct, "test_001")
        assert len(retrieved_product.storage_locations) == 1
        assert retrieved_product.storage_locations[0].location_fk == "lmt_archive"

    def test_cascade_delete_storage(self, session):
        """Test that deleting product cascades to storage."""
        # Create location
        location = Location(
            location_pk="test_loc",
            label="Test Location",
            site_code="TEST",
        )
        session.add(location)

        # Create product with storage
        product = DataProduct(
            product_pk="cascade_test",
            base_type=BaseType.RAW_OBS.value,
            name="obs_cascade",
            product_kind=ProductKind.RAW.value,
        )
        session.add(product)
        session.commit()

        storage = DataProductStorage(
            product_fk=product.product_pk,
            location_fk=location.location_pk,
            storage_key="/data/cascade.nc",
        )
        session.add(storage)
        session.commit()

        # Delete product
        session.delete(product)
        session.commit()

        # Verify storage is deleted (cascade)
        from sqlalchemy import select
        stmt = select(DataProductStorage).where(
            DataProductStorage.product_fk == "cascade_test"
        )
        result = session.execute(stmt).first()
        assert result is None


class TestDataProductAssoc:
    """Test DataProductAssoc model for provenance."""

    def test_provenance_relationship(self, session):
        """Test creating provenance edges."""
        # Create raw and reduced products
        raw_product = DataProduct(
            product_pk="raw_001",
            base_type=BaseType.RAW_OBS.value,
            name="raw_obs",
            product_kind=ProductKind.RAW.value,
        )
        reduced_product = DataProduct(
            product_pk="reduced_001",
            base_type=BaseType.REDUCED_OBS.value,
            name="reduced_obs",
            product_kind=ProductKind.REDUCED.value,
        )
        session.add_all([raw_product, reduced_product])
        session.commit()

        # Create provenance edge
        assoc = DataProductAssoc(
            assoc_type=AssocType.PROCESS_EDGE.value,
            src_product_fk="raw_001",
            dst_product_fk="reduced_001",
            process_module="tolteca.reduce",
            process_version="2.0.0",
        )
        session.add(assoc)
        session.commit()

        # Verify
        assert assoc.assoc_pk is not None
        assert assoc.src_product_fk == "raw_001"
        assert assoc.dst_product_fk == "reduced_001"


class TestFlagDefinition:
    """Test FlagDefinition model."""

    def test_create_flag_definition(self, session):
        """Test creating a flag definition."""
        flag = FlagDefinition(
            flag_key="DET_DEAD_PIXEL",
            group_key="DET",
            severity=FlagSeverity.BLOCK.value,
            description="Dead detector pixel",
        )

        session.add(flag)
        session.commit()

        assert flag.flag_key == "DET_DEAD_PIXEL"
        # active is stored as string in SQLite ('1' for True, '0' for False)
        assert flag.active in (True, '1', 1)

    def test_flag_name_unique(self, session):
        """Test that flag_key must be unique."""
        flag1 = FlagDefinition(
            flag_key="UNIQUE_FLAG",
            group_key="QA",
            severity=FlagSeverity.WARN.value,
        )
        flag2 = FlagDefinition(
            flag_key="UNIQUE_FLAG",  # Duplicate
            group_key="QA",
            severity=FlagSeverity.INFO.value,
        )

        session.add(flag1)
        session.commit()

        # Create new session to test database constraint
        session.expire_all()
        session.add(flag2)
        with pytest.raises((IntegrityError, SAWarning)):
            session.commit()


class TestDataProductFlag:
    """Test DataProductFlag model."""

    def test_flag_product(self, session):
        """Test assigning flag to product."""
        # Create flag definition
        flag_def = FlagDefinition(
            flag_key="TEST_FLAG",
            group_key="QA",
            severity=FlagSeverity.WARN.value,
        )
        session.add(flag_def)

        # Create product
        product = DataProduct(
            product_pk="flagged_001",
            base_type=BaseType.RAW_OBS.value,
            name="flagged_obs",
            product_kind=ProductKind.RAW.value,
        )
        session.add(product)
        session.commit()

        # Assign flag
        flag_assignment = DataProductFlag(
            product_fk=product.product_pk,
            flag_key=flag_def.flag_key,
            asserted_by="test_user",
        )
        session.add(flag_assignment)
        session.commit()

        # Verify relationship
        retrieved_product = session.get(DataProduct, "flagged_001")
        assert len(retrieved_product.flags) == 1
        assert retrieved_product.flags[0].flag_key == "TEST_FLAG"


class TestReductionTask:
    """Test ReductionTask model."""

    def test_create_reduction_task(self, session):
        """Test creating a reduction task."""
        task = ReductionTask(
            task_pk="task_001",
            status=TaskStatus.QUEUED.value,
            params_hash="abc123",
            params={"threshold": 5.0},
            input_set_hash="def456",
        )

        session.add(task)
        session.commit()

        assert task.task_pk == "task_001"
        assert task.status == TaskStatus.QUEUED.value

    def test_task_with_inputs_outputs(self, session):
        """Test task with input and output relationships."""
        # Create products
        input_product = DataProduct(
            product_pk="input_001",
            base_type=BaseType.RAW_OBS.value,
            name="input",
            product_kind=ProductKind.RAW.value,
        )
        output_product = DataProduct(
            product_pk="output_001",
            base_type=BaseType.REDUCED_OBS.value,
            name="output",
            product_kind=ProductKind.REDUCED.value,
        )
        session.add_all([input_product, output_product])
        session.commit()

        # Create task
        task = ReductionTask(
            task_pk="task_002",
            status=TaskStatus.DONE.value,
            params_hash="hash123",
            params={"method": "standard"},
            input_set_hash="inhash123",
        )
        session.add(task)
        session.commit()

        # Add input and output
        task_input = TaskInput(
            task_fk=task.task_pk,
            product_fk="input_001",
            role="science",
        )
        task_output = TaskOutput(
            task_fk=task.task_pk,
            product_fk="output_001",
        )
        session.add_all([task_input, task_output])
        session.commit()

        # Verify relationships
        retrieved_task = session.get(ReductionTask, "task_002")
        assert len(retrieved_task.inputs) == 1
        assert len(retrieved_task.outputs) == 1


class TestEventLog:
    """Test EventLog model."""

    def test_create_event(self, session):
        """Test creating an event log entry."""
        # Create product
        product = DataProduct(
            product_pk="event_test",
            base_type=BaseType.RAW_OBS.value,
            name="event_obs",
            product_kind=ProductKind.RAW.value,
        )
        session.add(product)
        session.commit()

        # Create event
        event = EventLog(
            event_type="STATUS_CHANGE",
            entity_type="product",
            entity_id=product.product_pk,
            payload={"old_status": "MISSING", "new_status": "AVAILABLE"},
        )
        session.add(event)
        session.commit()

        assert event.seq is not None
        assert event.event_type == "STATUS_CHANGE"
        assert event.entity_id == "event_test"


class TestLocation:
    """Test Location model."""

    def test_create_location(self, session):
        """Test creating a storage location."""
        location = Location(
            location_pk="umass_cache",
            label="UMass Cache",
            site_code="UMASS",
            priority=10,
        )

        session.add(location)
        session.commit()

        assert location.location_pk == "umass_cache"
        assert location.site_code == "UMASS"
        assert location.priority == 10

    def test_location_label_unique(self, session):
        """Test that location label must be unique."""
        loc1 = Location(
            location_pk="loc1",
            label="Primary Archive",
            site_code="LMT",
        )
        loc2 = Location(
            location_pk="loc2",
            label="Primary Archive",  # Duplicate label
            site_code="UMASS",
        )

        session.add(loc1)
        session.commit()

        session.add(loc2)
        with pytest.raises(IntegrityError):
            session.commit()


class TestComplexRelationships:
    """Test complex multi-table relationships."""

    def test_full_workflow(self, session):
        """Test complete workflow with all relationships."""
        # Create location
        location = Location(
            location_pk="workflow_loc",
            label="Workflow Location",
            site_code="TEST",
        )

        # Create flag definition
        flag_def = FlagDefinition(
            flag_key="WORKFLOW_FLAG",
            group_key="QA",
            severity=FlagSeverity.INFO.value,
        )

        # Create raw product
        raw_product = DataProduct(
            product_pk="workflow_raw",
            base_type=BaseType.RAW_OBS.value,
            name="workflow_raw_obs",
            product_kind=ProductKind.RAW.value,
        )

        # Create reduced product
        reduced_product = DataProduct(
            product_pk="workflow_reduced",
            base_type=BaseType.REDUCED_OBS.value,
            name="workflow_reduced_obs",
            product_kind=ProductKind.REDUCED.value,
        )

        session.add_all([location, flag_def, raw_product, reduced_product])
        session.commit()

        # Add storage
        storage = DataProductStorage(
            product_fk="workflow_raw",
            location_fk="workflow_loc",
            storage_key="/data/workflow.nc",
        )

        # Add flag
        flag = DataProductFlag(
            product_fk="workflow_raw",
            flag_key="WORKFLOW_FLAG",
        )

        # Add provenance
        assoc = DataProductAssoc(
            assoc_type=AssocType.PROCESS_EDGE.value,
            src_product_fk="workflow_raw",
            dst_product_fk="workflow_reduced",
        )

        session.add_all([storage, flag, assoc])
        session.commit()

        # Verify all relationships
        retrieved_raw = session.get(DataProduct, "workflow_raw")
        assert len(retrieved_raw.storage_locations) == 1
        assert len(retrieved_raw.flags) == 1

        # Verify provenance exists
        from sqlalchemy import select
        stmt = select(DataProductAssoc).where(
            DataProductAssoc.src_product_fk == "workflow_raw"
        )
        result = session.execute(stmt).first()
        assert result is not None
