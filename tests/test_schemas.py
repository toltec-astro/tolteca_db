"""Tests for Pydantic schemas (CLI/API boundaries)."""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from tolteca_db.models import (
    DataProduct,
    DataProductCreate,
    DataProductFlagCreate,
    DataProductResponse,
    FlagDefinition,
    FlagDefinitionCreate,
    FlagDefinitionResponse,
    Location,
    LocationCreate,
    LocationResponse,
    ReductionTaskCreate,
)
from tolteca_db.utils import product_id_hash, utc_now


class TestDataProductCreate:
    """Test DataProductCreate input validation."""

    def test_valid_raw_obs(self) -> None:
        """Test creating valid raw observation product."""
        data = DataProductCreate(
            base_type="dp_raw_obs",
            name="toltec_12345_00_00_00",
            meta={"obs_id": "12345", "master": "TEL"},
        )
        assert data.base_type == "dp_raw_obs"
        assert data.name == "toltec_12345_00_00_00"
        assert data.product_kind == "RAW"
        assert data.status == "ACTIVE"
        assert data.level == 0

    def test_valid_reduced_obs(self) -> None:
        """Test creating valid reduced observation product."""
        data = DataProductCreate(
            base_type="dp_reduced_obs",
            name="reduced_12345_00",
            level=1,
            product_kind="REDUCED",
            meta={"reduction_version": "1.0"},
        )
        assert data.base_type == "dp_reduced_obs"
        assert data.level == 1
        assert data.product_kind == "REDUCED"

    def test_invalid_base_type_empty(self) -> None:
        """Test validation fails for empty base_type."""
        with pytest.raises(ValidationError) as exc_info:
            DataProductCreate(base_type="", name="test")
        assert "base_type" in str(exc_info.value)

    def test_invalid_product_kind(self) -> None:
        """Test validation fails for invalid product_kind."""
        with pytest.raises(ValidationError) as exc_info:
            DataProductCreate(
                base_type="dp_raw_obs",
                name="test",
                product_kind="INVALID",
            )
        assert "product_kind" in str(exc_info.value)

    def test_invalid_status(self) -> None:
        """Test validation fails for invalid status."""
        with pytest.raises(ValidationError) as exc_info:
            DataProductCreate(
                base_type="dp_raw_obs",
                name="test",
                status="DELETED",
            )
        assert "status" in str(exc_info.value)

    def test_invalid_level_negative(self) -> None:
        """Test validation fails for negative level."""
        with pytest.raises(ValidationError) as exc_info:
            DataProductCreate(
                base_type="dp_raw_obs",
                name="test",
                level=-1,
            )
        assert "level" in str(exc_info.value)

    def test_meta_defaults_to_empty_dict(self) -> None:
        """Test meta defaults to empty dict."""
        data = DataProductCreate(base_type="dp_raw_obs", name="test")
        assert data.meta == {}

    def test_convert_to_orm(self) -> None:
        """Test converting Pydantic → ORM."""
        data = DataProductCreate(
            base_type="dp_raw_obs",
            name="toltec_12345_00_00_00",
            meta={"obs_id": "12345"},
        )
        # Generate product_pk - identity includes name and meta
        identity = {"name": data.name, **data.meta}
        product_pk = product_id_hash(data.base_type, identity)

        # Create ORM object from Pydantic data
        orm_obj = DataProduct(product_pk=product_pk, **data.model_dump())

        assert orm_obj.product_pk == product_pk
        assert orm_obj.base_type == "dp_raw_obs"
        assert orm_obj.name == "toltec_12345_00_00_00"
        assert orm_obj.meta == {"obs_id": "12345"}


class TestLocationCreate:
    """Test LocationCreate input validation."""

    def test_valid_location(self) -> None:
        """Test creating valid location."""
        data = LocationCreate(
            label="LMT Archive",
            site_code="LMT",
            priority=10,
            meta={"base_path": "/data/toltec"},
        )
        assert data.label == "LMT Archive"
        assert data.site_code == "LMT"
        assert data.priority == 10

    def test_invalid_label_empty(self) -> None:
        """Test validation fails for empty label."""
        with pytest.raises(ValidationError) as exc_info:
            LocationCreate(label="")
        assert "label" in str(exc_info.value)

    def test_invalid_priority_negative(self) -> None:
        """Test validation fails for negative priority."""
        with pytest.raises(ValidationError) as exc_info:
            LocationCreate(label="Test", priority=-1)
        assert "priority" in str(exc_info.value)

    def test_site_code_optional(self) -> None:
        """Test site_code is optional."""
        data = LocationCreate(label="Local Storage")
        assert data.site_code is None


class TestFlagDefinitionCreate:
    """Test FlagDefinitionCreate input validation."""

    def test_valid_flag(self) -> None:
        """Test creating valid flag definition."""
        data = FlagDefinitionCreate(
            flag_key="DET_DEAD_PIXEL",
            group_key="DET",
            severity="BLOCK",
            description="Detector has dead pixel",
        )
        assert data.flag_key == "DET_DEAD_PIXEL"
        assert data.group_key == "DET"
        assert data.severity == "BLOCK"

    def test_invalid_flag_key_pattern(self) -> None:
        """Test validation fails for invalid flag_key pattern."""
        with pytest.raises(ValidationError) as exc_info:
            FlagDefinitionCreate(
                flag_key="det-dead-pixel",  # Should be uppercase with underscores
                group_key="DET",
                severity="BLOCK",
                description="Test",
            )
        assert "flag_key" in str(exc_info.value)

    def test_invalid_group_key_pattern(self) -> None:
        """Test validation fails for invalid group_key pattern."""
        with pytest.raises(ValidationError) as exc_info:
            FlagDefinitionCreate(
                flag_key="DET_DEAD",
                group_key="det",  # Should be uppercase
                severity="BLOCK",
                description="Test",
            )
        assert "group_key" in str(exc_info.value)

    def test_invalid_severity(self) -> None:
        """Test validation fails for invalid severity."""
        with pytest.raises(ValidationError) as exc_info:
            FlagDefinitionCreate(
                flag_key="DET_DEAD",
                group_key="DET",
                severity="SEVERE",  # Not in allowed values
                description="Test",
            )
        assert "severity" in str(exc_info.value)

    def test_active_defaults_to_true(self) -> None:
        """Test active defaults to True."""
        data = FlagDefinitionCreate(
            flag_key="DET_DEAD",
            group_key="DET",
            severity="BLOCK",
            description="Test",
        )
        assert data.active is True


class TestDataProductFlagCreate:
    """Test DataProductFlagCreate input validation."""

    def test_valid_flag_assignment(self) -> None:
        """Test creating valid flag assignment."""
        data = DataProductFlagCreate(
            product_pk="product_pk_123",
            flag_key="DET_DEAD_PIXEL",
            asserted_by="qa_pipeline",
            details={"pixel_index": 42},
        )
        assert data.product_pk == "product_pk_123"
        assert data.flag_key == "DET_DEAD_PIXEL"
        assert data.asserted_by == "qa_pipeline"
        assert data.details == {"pixel_index": 42}

    def test_details_defaults_to_empty_dict(self) -> None:
        """Test details defaults to empty dict."""
        data = DataProductFlagCreate(
            product_pk="product_pk_123",
            flag_key="DET_DEAD_PIXEL",
        )
        assert data.details == {}


class TestReductionTaskCreate:
    """Test ReductionTaskCreate input validation."""

    def test_valid_task(self) -> None:
        """Test creating valid reduction task."""
        data = ReductionTaskCreate(
            params={"algorithm": "standard", "threshold": 0.5},
            input_product_pks=["pk_1", "pk_2", "pk_3"],
        )
        assert data.params == {"algorithm": "standard", "threshold": 0.5}
        assert len(data.input_product_pks) == 3

    def test_invalid_empty_inputs(self) -> None:
        """Test validation fails for empty input list."""
        with pytest.raises(ValidationError) as exc_info:
            ReductionTaskCreate(params={}, input_product_pks=[])
        assert "input_product_pks" in str(exc_info.value)


class TestDataProductResponse:
    """Test DataProductResponse export schema."""

    def test_orm_to_pydantic_conversion(self) -> None:
        """Test converting ORM object to Pydantic response."""
        # Create ORM object
        now = utc_now()
        orm_obj = DataProduct(
            product_pk="product_pk_123",
            base_type="dp_raw_obs",
            name="test_obs",
            level=0,
            product_kind="RAW",
            status="ACTIVE",
            meta={"obs_id": "12345"},
            created_at=now,
            updated_at=now,
        )

        # Convert to Pydantic
        response = DataProductResponse.model_validate(orm_obj)

        assert response.product_pk == "product_pk_123"
        assert response.base_type == "dp_raw_obs"
        assert response.name == "test_obs"
        assert response.level == 0
        assert response.product_kind == "RAW"
        assert response.status == "ACTIVE"
        assert response.meta == {"obs_id": "12345"}
        assert isinstance(response.created_at, datetime)

    def test_json_export(self) -> None:
        """Test exporting to JSON."""
        now = utc_now()
        orm_obj = DataProduct(
            product_pk="product_pk_123",
            base_type="dp_raw_obs",
            name="test_obs",
            level=0,
            product_kind="RAW",
            status="ACTIVE",
            meta={"obs_id": "12345"},
            created_at=now,
            updated_at=now,
        )

        response = DataProductResponse.model_validate(orm_obj)
        json_str = response.model_dump_json()

        assert "product_pk_123" in json_str
        assert "dp_raw_obs" in json_str
        assert "obs_id" in json_str


class TestLocationResponse:
    """Test LocationResponse export schema."""

    def test_orm_to_pydantic_conversion(self) -> None:
        """Test converting ORM Location to Pydantic."""
        now = utc_now()
        orm_obj = Location(
            location_pk="loc_lmt",
            label="LMT Archive",
            site_code="LMT",
            priority=10,
            meta={"base_path": "/data"},
            created_at=now,
        )

        response = LocationResponse.model_validate(orm_obj)

        assert response.location_pk == "loc_lmt"
        assert response.label == "LMT Archive"
        assert response.site_code == "LMT"
        assert response.priority == 10


class TestFlagDefinitionResponse:
    """Test FlagDefinitionResponse export schema."""

    def test_orm_to_pydantic_conversion(self) -> None:
        """Test converting ORM FlagDefinition to Pydantic."""
        orm_obj = FlagDefinition(
            flag_key="DET_DEAD_PIXEL",
            group_key="DET",
            severity="BLOCK",
            description="Detector has dead pixel",
            active=True,
        )

        response = FlagDefinitionResponse.model_validate(orm_obj)

        assert response.flag_key == "DET_DEAD_PIXEL"
        assert response.group_key == "DET"
        assert response.severity == "BLOCK"
        assert response.active is True


class TestRoundTripConversion:
    """Test Pydantic → ORM → Pydantic round-trip conversions."""

    def test_data_product_round_trip(self) -> None:
        """Test DataProduct round-trip conversion."""
        # Start with Pydantic input schema
        create_data = DataProductCreate(
            base_type="dp_raw_obs",
            name="test_obs",
            meta={"obs_id": "12345"},
        )

        # Convert to ORM
        identity = {"name": create_data.name, **create_data.meta}
        product_pk = product_id_hash(create_data.base_type, identity)
        now = utc_now()
        orm_obj = DataProduct(
            product_pk=product_pk,
            created_at=now,
            updated_at=now,
            **create_data.model_dump(),
        )

        # Convert back to Pydantic response
        response = DataProductResponse.model_validate(orm_obj)

        # Verify data preserved
        assert response.base_type == create_data.base_type
        assert response.name == create_data.name
        assert response.meta == create_data.meta
        assert response.product_kind == create_data.product_kind
        assert response.status == create_data.status
