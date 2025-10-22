"""Pydantic schemas for CLI/API boundaries.

These schemas are used ONLY at external boundaries (CLI input validation,
JSON/YAML export). Internal operations should use ORM objects directly.

Design Pattern
--------------
- ORM objects for internal operations (service, repository layers)
- Pydantic for validation at boundaries (CLI input, export formats)
- Use `from_attributes=True` (ConfigDict) to convert ORM → Pydantic

Examples
--------
CLI Input Validation:
    >>> from tolteca_db.models.schemas import DataProductCreate
    >>> # User provides data via CLI
    >>> data = DataProductCreate(base_type="dp_raw_obs", name="obs_12345")
    >>> # Pydantic validates input
    >>> product_orm = DataProduct(**data.model_dump())

Export to JSON:
    >>> from tolteca_db.models.schemas import DataProductResponse
    >>> # ORM object from database
    >>> product_orm = repo.get("product_pk_123")
    >>> # Convert to Pydantic for export
    >>> response = DataProductResponse.model_validate(product_orm)
    >>> print(response.model_dump_json(indent=2))
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    pass

__all__ = [
    # Input schemas (CLI validation)
    "DataProductCreate",
    "LocationCreate",
    "FlagDefinitionCreate",
    "DataProductFlagCreate",
    "ReductionTaskCreate",
    # Response schemas (export)
    "DataProductResponse",
    "DataProductStorageResponse",
    "LocationResponse",
    "FlagDefinitionResponse",
    "DataProductFlagResponse",
    "ReductionTaskResponse",
    "EventLogResponse",
]


# ============================================================================
# Input Schemas (CLI Validation)
# ============================================================================


class DataProductCreate(BaseModel):
    """
    Schema for creating a DataProduct via CLI.

    Validates user input before creating ORM object.

    Examples
    --------
    >>> data = DataProductCreate(
    ...     base_type="dp_raw_obs",
    ...     name="toltec_12345_00_00_00",
    ...     meta={"obs_id": "12345", "master": "TEL"}
    ... )
    >>> product = DataProduct(**data.model_dump())
    """

    base_type: str = Field(
        ...,
        min_length=1,
        max_length=32,
        description="Product type (dp_raw_obs, dp_reduced_obs, etc.)",
    )
    subtype: str | None = Field(
        None,
        max_length=32,
        description="Optional subtype classification",
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Product name (unique within base_type)",
    )
    level: int = Field(
        0,
        ge=0,
        description="Processing level (0=raw, 1+=reduced)",
    )
    product_kind: str = Field(
        "RAW",
        pattern="^(RAW|REDUCED)$",
        description="Product kind classification",
    )
    status: str = Field(
        "ACTIVE",
        pattern="^(ACTIVE|SUPERSEDED)$",
        description="Product lifecycle status",
    )
    availability_state: str | None = Field(
        "AVAILABLE",
        pattern="^(AVAILABLE|MISSING|REMOTE|STAGED)$",
        description="Availability state",
    )
    content_hash: str | None = Field(
        None,
        max_length=128,
        description="Content hash (blake3/sha256)",
    )
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Flexible metadata JSON",
    )


class LocationCreate(BaseModel):
    """Schema for creating a Location via CLI."""

    label: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Human-readable location label",
    )
    site_code: str | None = Field(
        None,
        max_length=16,
        description="Site code (LMT, UMass, etc.)",
    )
    priority: int = Field(
        0,
        ge=0,
        description="Access priority (higher = preferred)",
    )
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Location metadata",
    )


class FlagDefinitionCreate(BaseModel):
    """Schema for creating a FlagDefinition via CLI."""

    flag_key: str = Field(
        ...,
        min_length=1,
        max_length=64,
        pattern="^[A-Z_]+$",
        description="Flag key (DET_*, TEL_*, QA_*, etc.)",
    )
    group_key: str = Field(
        ...,
        max_length=16,
        pattern="^[A-Z]+$",
        description="Flag group (DET, TEL, QA, CAL, ING)",
    )
    severity: str = Field(
        ...,
        pattern="^(INFO|WARN|BLOCK|CRITICAL)$",
        description="Flag severity level",
    )
    description: str = Field(
        ...,
        min_length=1,
        description="Human-readable description",
    )
    active: bool = Field(
        True,
        description="Whether flag is currently active",
    )


class DataProductFlagCreate(BaseModel):
    """Schema for flagging a product via CLI."""

    product_pk: str = Field(
        ...,
        description="Product primary key to flag",
    )
    flag_key: str = Field(
        ...,
        description="Flag key to assign",
    )
    asserted_by: str | None = Field(
        None,
        description="User/process asserting the flag",
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional flag context",
    )


class ReductionTaskCreate(BaseModel):
    """Schema for creating a ReductionTask via CLI."""

    params: dict[str, Any] = Field(
        ...,
        description="Task parameters (JSON)",
    )
    input_product_pks: list[str] = Field(
        ...,
        min_length=1,
        description="List of input product primary keys",
    )


# ============================================================================
# Response Schemas (Export to JSON/YAML)
# ============================================================================


class DataProductResponse(BaseModel):
    """
    Schema for exporting DataProduct to JSON/YAML.

    Use `model_validate(orm_obj)` to convert ORM → Pydantic.

    Examples
    --------
    >>> product_orm = repo.get("product_pk_123")
    >>> response = DataProductResponse.model_validate(product_orm)
    >>> print(response.model_dump_json(indent=2))
    """

    model_config = ConfigDict(from_attributes=True)

    product_pk: str
    base_type: str
    subtype: str | None
    name: str
    level: int
    product_kind: str
    status: str
    availability_state: str | None
    content_hash: str | None
    meta: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class DataProductStorageResponse(BaseModel):
    """Schema for exporting DataProductStorage."""

    model_config = ConfigDict(from_attributes=True)

    product_fk: str
    location_fk: str
    storage_key: str
    role: str
    availability_state: str | None
    size: int | None
    checksum: str | None
    last_verified_at: datetime | None
    created_at: datetime


class LocationResponse(BaseModel):
    """Schema for exporting Location."""

    model_config = ConfigDict(from_attributes=True)

    location_pk: str
    label: str
    site_code: str | None
    priority: int
    meta: dict[str, Any]
    created_at: datetime


class FlagDefinitionResponse(BaseModel):
    """Schema for exporting FlagDefinition."""

    model_config = ConfigDict(from_attributes=True)

    flag_key: str
    group_key: str
    severity: str
    description: str
    active: bool


class DataProductFlagResponse(BaseModel):
    """Schema for exporting DataProductFlag."""

    model_config = ConfigDict(from_attributes=True)

    product_fk: str
    flag_key: str
    asserted_at: datetime
    asserted_by: str | None
    details: dict[str, Any]


class ReductionTaskResponse(BaseModel):
    """Schema for exporting ReductionTask."""

    model_config = ConfigDict(from_attributes=True)

    task_pk: str
    status: str
    params_hash: str
    params: dict[str, Any]
    input_set_hash: str
    worker_host: str | None
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    created_at: datetime


class EventLogResponse(BaseModel):
    """Schema for exporting EventLog."""

    model_config = ConfigDict(from_attributes=True)

    seq: int
    entity_type: str
    entity_id: str
    event_type: str
    event_data: dict[str, Any]
    created_at: datetime
