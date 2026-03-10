"""Asset definitions for Dagster pipeline.

Implements the asset dependency chain:
    raw_obs_metadata → raw_obs_product → parquet_files → association_groups

All assets follow Dagster 2025 best practices:
- @asset decorator (not ops)
- Helper functions for queries (plain Python, not @op)
- Proper Output with metadata
- Idempotent operations

References
----------
- Dagster Assets: https://docs.dagster.io/guides/build/assets/defining-assets
"""

# CRITICAL: Cannot use `from __future__ import annotations` with Dagster
#
# PEP 563 (implemented via `from __future__ import annotations`) converts all
# type annotations to strings for deferred evaluation. However, Dagster's
# runtime type validation requires actual type objects, not string representations.
#
# When annotations are strings, Dagster's context type checker fails:
#   DagsterInvalidDefinitionError: Cannot annotate `context` parameter with
#   type AssetExecutionContext. `context` must be annotated with
#   AssetExecutionContext, OpExecutionContext, or left blank.
#
# This is because the validator checks:
#   if params[0].annotation not in [AssetExecutionContext, ...]
# And "AssetExecutionContext" (string) != AssetExecutionContext (class object)
#
# See: https://github.com/dagster-io/dagster/issues/28342
# Related: PEP 563, Dagster MIGRATION.md (AssetExecutionContext type hints)

__all__ = [
    "raw_obs_product",
    "quartet_complete",
    "parquet_files",
    "association_groups",
]

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    Output,
    asset,
)
from dagster.core.errors import DagsterExecutionInterruptedError

from .partitions import (
    quartet_interface_partitions,
    quartet_partitions,
    get_interface_roach_index,
    get_array_name_for_interface,
)
from tolteca_db.models.metadata import RawObsMeta
from tolteca_db.utils.uid import make_raw_obs_uid, parse_raw_obs_uid


@asset(
    partitions_def=quartet_interface_partitions,
    description="Ingest interface data file using DataIngestor (creates DataProd + DataProdSource)",
    compute_kind="database",
    required_resource_keys={"toltec_db", "tolteca_db", "location"},
)
def raw_obs_product(
    context: AssetExecutionContext,
) -> dict:
    """
    Ingest interface data file into tolteca_db using DataIngestor.
    
    Uses the existing ingest module to properly create:
    - ONE DataProd per quartet (shared across all interfaces)
    - ONE DataProdSource per interface file (links file to DataProd + Location)
    
    This ensures the correct architecture:
    - DataProd = logical quartet (master-obsnum-subobsnum-scannum)
    - DataProdSource = physical files (one per interface)
    - Location = storage location (LMT site)
    
    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context with 2D partition_key
    
    Returns
    -------
    dict
        Ingestion result with keys:
        - data_prod_pk: str - DataProd primary key
        - source_uri: str - DataProdSource URI
        - created: bool - Whether new entries were created
        - skipped: bool - Whether entries already existed
    
    Raises
    ------
    DagsterExecutionInterruptedError
        If interface Valid flag is 0 (data acquisition in progress).
    ValueError
        If Location not found or file cannot be parsed
    
    Examples
    --------
    Asset materialization with 2D partition:
    >>> # dagster asset materialize --select raw_obs_product \\
    >>> #   --partition quartet=toltec-1-123456-0-0 interface=toltec5
    
    Notes
    -----
    - **2D Partitions**: quartet × interface (1-1 mapping to toltec_db rows)
    - **Architecture**: ONE DataProd per quartet + multiple DataProdSource (one per interface)
    - **Uses DataIngestor**: Reuses existing tested ingestion logic
    - **Location**: Links files to Location registry (LMT site)
    - **Validation Check**: Raises DagsterExecutionInterrupt if Valid=0
    - **Idempotent**: Safe to re-run, skips existing entries
    """
    from tolteca_db.dagster.helpers import ingest_interface_from_toltec_db

    # Parse 2D partition key
    partition_key = context.partition_key
    if hasattr(partition_key, "keys_by_dimension"):
        quartet_key = partition_key.keys_by_dimension["quartet"]
        interface = partition_key.keys_by_dimension["quartet_interface"]
    else:
        quartet_key = partition_key["quartet"]
        interface = partition_key["quartet_interface"]

    identity = parse_raw_obs_uid(quartet_key)
    roach_index = get_interface_roach_index(interface)
    array_name = get_array_name_for_interface(interface)

    context.log.info(f"Processing {quartet_key} / {interface} ({array_name})")

    # Get resources
    toltec_db = context.resources.toltec_db
    tolteca_db = context.resources.tolteca_db
    location = context.resources.location

    # Get data root from location config
    data_root = location.get_data_root()
    if data_root is None:
        raise ValueError("location.data_root is not configured")

    with (
        toltec_db.get_session() as toltec_session,
        tolteca_db.get_session() as tolteca_session,
    ):
        result = ingest_interface_from_toltec_db(
            master=identity["master"],
            obsnum=identity["obsnum"],
            subobsnum=identity["subobsnum"],
            scannum=identity["scannum"],
            roach_index=roach_index,
            toltec_session=toltec_session,
            tolteca_session=tolteca_session,
            data_root=data_root,
            location_label=location.location_pk,
        )

    if result["created"]:
        context.log.info(
            f"  ✓ Ingested: DataProd={result['data_prod_pk']}, Source={result['source_uri']}"
        )
    else:
        context.log.info(
            f"  ⊙ Skipped (already exists): DataProd={result['data_prod_pk']}"
        )

    # Log metadata for observability
    context.add_output_metadata(
        {
            "data_prod_pk": result["data_prod_pk"],
            "source_uri": result["source_uri"],
            "quartet_key": quartet_key,
            "interface": interface,
            "roach_index": MetadataValue.int(roach_index),
            "array_name": array_name,
            "master": identity["master"],
            "obsnum": MetadataValue.int(identity["obsnum"]),
            "subobsnum": MetadataValue.int(identity["subobsnum"]),
            "scannum": MetadataValue.int(identity["scannum"]),
            "created": MetadataValue.bool(result["created"]),
            "skipped": MetadataValue.bool(result["skipped"]),
            "filename": result.get("filename", "unknown"),
        }
    )

    return result


# Old raw_obs_metadata asset removed - functionality consolidated into raw_obs_product above


@asset(
    partitions_def=quartet_partitions,
    description="Verify all 13 interfaces valid for quartet (aggregation gate)",
    deps=[
        AssetDep(
            raw_obs_product,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="quartet"
            ),
        )
    ],
    compute_kind="validation",
    required_resource_keys={"toltec_db", "validation"},
    automation_condition=(
        AutomationCondition.eager().without(~AutomationCondition.any_deps_missing())
        & AutomationCondition.missing()  # Only execute for unmaterialized partitions
    ).with_label("eager_allow_missing_skip_completed"),
    output_required=False,
)
def quartet_complete(context: AssetExecutionContext):
    """
    Aggregation asset verifying all 13 interfaces are valid for a quartet.
    
    This asset acts as a gate for downstream processing, ensuring that all
    13 TolTEC interfaces (toltec0-12) have completed validation (Valid=1)
    before allowing parquet conversion and further analysis.
    
    Uses Dagster's declarative automation with customized eager condition:
    - Uses AutomationCondition.eager().without(~any_deps_missing()) pattern
    - Allows execution even when some upstream deps missing (disabled interfaces)
    - Checks timeout and enabled interfaces within asset execution logic
    - No custom sensor needed - Dagster handles dependency tracking
    
    Timeout logic is handled within the asset execution:
    - Checks database for validation status and timing
    - Determines which interfaces are enabled based on database state
    - Uses output_required=False to conditionally skip output if not ready
    - Raises DagsterExecutionInterruptedError to retry if incomplete
    
    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context with 1D partition (quartet only)
    
    Returns
    -------
    Output or None
        Dagster Output containing aggregated status dict with keys:
        - valid_count: int - Number of valid interfaces
        - expected_count: int - Total expected interfaces (accounting for disabled)
        - all_expected_valid: bool - True if all expected interfaces are valid
        - time_since_last_valid: float - Time since last Valid=1 transition
        - new_quartet_detected: bool - Whether a newer quartet exists
        Returns None if quartet not ready yet (will be retried by Dagster)
    
    Raises
    ------
    DagsterExecutionInterruptedError
        If not all interfaces are valid yet (will retry later)
    
    Examples
    --------
    Asset runs after each raw_obs_product materialization:
    >>> # dagster asset materialize --select quartet_complete \\
    >>> #   --partition toltec-1-123456-0-0
    
    Notes
    -----
    - **1D Partitioned**: quartet only (no interface dimension)
    - **Depends on**: All 13 raw_obs_product interface assets
    - **Declarative Automation**: eager().without(~any_deps_missing()) allows disabled interfaces
    - **Race Condition Fixed**: Waits for upstream deps but allows missing ones (disabled interfaces)
    - **Enabled Interface Detection**: Cannot know disabled interfaces in advance - determined by timeout
    - **Retry Behavior**: Raises DagsterExecutionInterruptedError if incomplete
    - **Timeout Check**: Evaluates completion criteria from database state
    - **Acts as Gate**: Downstream assets (parquet_files) depend on this
    """
    from tolteca_db.dagster.helpers import query_toltec_db_quartet_status

    # Parse 1D partition key (quartet only)
    partition_key = context.partition_key
    identity = parse_raw_obs_uid(partition_key)

    context.log.info(f"Checking quartet completion for {partition_key}")

    # Get database session from toltec_db resource
    toltec_db = context.resources.toltec_db

    # Query status of all 13 interfaces
    with toltec_db.get_session() as session:
        status = query_toltec_db_quartet_status(
            master=identity["master"],
            obsnum=identity["obsnum"],
            subobsnum=identity["subobsnum"],
            scannum=identity["scannum"],
            session=session,
        )

    valid_count = status["valid_count"]
    total_found = status["total_found"]
    time_since_last_valid = status["time_since_last_valid"]
    new_quartet_detected = status["new_quartet_detected"]

    # Get validation config from resource
    validation = context.resources.validation
    validation_timeout_seconds = validation.validation_timeout_seconds

    # Get expected interfaces (accounting for disabled interfaces)
    expected_interfaces = validation.get_expected_interfaces()
    valid_interfaces_set = set(status["valid_interfaces"])

    # Calculate which expected interfaces are missing
    missing_expected = expected_interfaces - valid_interfaces_set

    # Log interface status details
    context.log.info(
        f"Interface status: {valid_count}/{len(expected_interfaces)} expected valid "
        f"(total found: {total_found}, disabled: {validation.disabled_interfaces}) "
        f"({time_since_last_valid:.1f}s since last Valid=1)"
    )

    if status["valid_interfaces"]:
        context.log.info(f"  ✓ Valid interfaces: {status['valid_interfaces']}")

    if status["invalid_interfaces"]:
        context.log.info(f"  ⏳ Pending interfaces: {status['invalid_interfaces']}")

    if status["missing_interfaces"]:
        context.log.info(f"  ℹ Not found: {status['missing_interfaces']}")

    if validation.disabled_interfaces:
        context.log.info(
            f"  🚫 Disabled (configured): {validation.disabled_interfaces}"
        )

    if missing_expected:
        context.log.info(f"  ⚠ Missing expected interfaces: {sorted(missing_expected)}")

    # Completion criteria (either condition triggers completion):
    # 1. New quartet detected (definitive signal from data acquisition system)
    # 2. All expected interfaces are valid (accounting for disabled interfaces)
    # 3. Timeout: no new Valid=1 transitions within window (timer resets on each Valid=0→1)
    all_expected_valid = len(missing_expected) == 0

    is_complete = (
        new_quartet_detected  # Definitive completion signal
        or all_expected_valid  # All expected interfaces are valid
        or (
            valid_count > 0  # At least one interface must be valid
            and time_since_last_valid >= validation_timeout_seconds
        )
    )

    if not is_complete:
        if valid_count == 0:
            msg = (
                f"Quartet {partition_key} not started yet. "
                f"No interfaces are Valid=1. Waiting for data acquisition to begin."
            )
        else:
            msg = (
                f"Quartet {partition_key} still validating. "
                f"Valid: {valid_count}/{len(expected_interfaces)} expected interfaces. "
                f"(Found {total_found} total, {len(validation.disabled_interfaces)} disabled) "
                f"Time since last Valid=1: {time_since_last_valid:.1f}s "
                f"(timeout threshold: {validation_timeout_seconds:.1f}s). "
                f"Missing expected: {sorted(missing_expected)}. "
                f"Will retry to check for more interfaces."
            )
        context.log.info(msg)
        raise DagsterExecutionInterruptedError(msg)

    # Observation is complete!
    completion_reason = (
        "new quartet detected"
        if new_quartet_detected
        else "all expected interfaces valid"
        if all_expected_valid
        else "timeout reached"
    )
    context.log.info(
        f"✓ Quartet {partition_key} complete! "
        f"{valid_count}/{len(expected_interfaces)} expected interfaces validated. "
        f"Completion reason: {completion_reason}. "
        f"Time since last Valid=1: {time_since_last_valid:.1f}s "
        f"(threshold: {validation_timeout_seconds:.1f}s)"
    )

    return Output(
        value={
            "valid_count": valid_count,
            "total_found": total_found,
            "expected_count": len(expected_interfaces),
            "disabled_count": len(validation.disabled_interfaces),
            "is_complete": is_complete,
            "all_expected_valid": all_expected_valid,
            "time_since_last_valid": time_since_last_valid,
            "new_quartet_detected": new_quartet_detected,
        },
        metadata={
            "partition_key": partition_key,
            "valid_count": MetadataValue.int(valid_count),
            "total_found": MetadataValue.int(total_found),
            "expected_count": MetadataValue.int(len(expected_interfaces)),
            "disabled_interfaces": MetadataValue.text(
                ", ".join(map(str, sorted(validation.disabled_interfaces)))
            ),
            "is_complete": MetadataValue.bool(is_complete),
            "all_expected_valid": MetadataValue.bool(all_expected_valid),
            "time_since_last_valid": MetadataValue.float(time_since_last_valid),
            "validation_timeout_seconds": MetadataValue.float(
                validation_timeout_seconds
            ),
            "valid_interfaces": MetadataValue.text(
                ", ".join(map(str, status["valid_interfaces"]))
            ),
            "missing_expected_interfaces": MetadataValue.text(
                ", ".join(map(str, sorted(missing_expected)))
                if missing_expected
                else "none"
            ),
            "first_valid_time": status.get("first_valid_time", "unknown"),
            "last_valid_time": status.get("last_valid_time", "unknown"),
            "new_quartet_detected": MetadataValue.bool(new_quartet_detected),
        },
    )


@asset(
    partitions_def=quartet_partitions,
    description="Convert NetCDF observation data to Parquet format",
    deps=[quartet_complete],
    compute_kind="compute",
)
def parquet_files(
    context: AssetExecutionContext,
    quartet_complete: dict,
) -> dict[str, str]:
    """
    Convert raw observation NetCDF files to Parquet format.
    
    This asset reads NetCDF files from the raw observation data
    and converts them to Parquet format for efficient querying
    and analysis.
    
    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context
    raw_obs_product : str
        DataProd PK from upstream asset
    
    Returns
    -------
    dict[str, str]
        Mapping of array names to Parquet file paths
    
    Examples
    --------
    >>> # Asset automatically runs after raw_obs_product
    >>> # dagster asset materialize --select parquet_files \\
    >>> #   --partition toltec-123456-0-1
    
    Notes
    -----
    - Converts TolTEC detector array data to Parquet
    - Uses DuckDB for efficient conversion
    - Stores results in scratch/ directory
    - Idempotent: checks for existing Parquet files
    """
    partition_key = context.partition_key
    identity = parse_raw_obs_uid(partition_key)

    context.log.info(f"Converting NetCDF to Parquet for {partition_key}")

    # TODO: Implement NetCDF → Parquet conversion
    # 1. Locate NetCDF files using raw_obs_product
    # 2. Read NetCDF data with xarray or netCDF4
    # 3. Convert to DuckDB/Parquet using to_parquet()
    # 4. Store in scratch/ directory
    # 5. Return file paths

    # Placeholder implementation
    parquet_files_map = {
        "a1100": f"scratch/parquet/{partition_key}_a1100.parquet",
        "a1400": f"scratch/parquet/{partition_key}_a1400.parquet",
        "a2000": f"scratch/parquet/{partition_key}_a2000.parquet",
    }

    context.log.info(f"Created {len(parquet_files_map)} Parquet files")

    return Output(
        value=parquet_files_map,
        metadata={
            "partition_key": partition_key,
            "num_files": MetadataValue.int(len(parquet_files_map)),
            "arrays": MetadataValue.text(", ".join(parquet_files_map.keys())),
        },
    )


@asset(
    description="Generate observation association groups",
    compute_kind="database",
)
def association_groups(
    context: AssetExecutionContext,
) -> int:
    """
    Generate association groups linking related observations.

    This asset analyzes observations across all partitions to identify
    related observations (e.g., same source, same project) and creates
    association group entries in the database.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context

    Returns
    -------
    int
        Number of association groups created or updated

    Examples
    --------
    >>> # Non-partitioned asset runs across all observations
    >>> # dagster asset materialize --select association_groups

    Notes
    -----
    - Non-partitioned: operates on entire observation set
    - Incremental: only processes new observations since last run
    - Creates AssocGroup entries linking related observations
    - Uses obs_goal, source_name, and temporal proximity
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session

    context.log.info("Generating association groups")

    # TODO: Implement association group generation
    # 1. Query all recent observations from tolteca_db
    # 2. Group by (obs_goal, source_name)
    # 3. Apply temporal proximity rules
    # 4. Create/update AssocGroup entries
    # 5. Link observations to groups via association table

    # Placeholder implementation
    engine = get_engine()
    with Session(engine) as session:
        # Query observations and group them
        # For now, just return a count
        num_groups = 0  # Placeholder

        context.log.info(f"Processed {num_groups} association groups")

        return Output(
            value=num_groups,
            metadata={
                "num_groups": MetadataValue.int(num_groups),
                "timestamp": MetadataValue.timestamp(
                    context.dagster_run.start_time or 0
                ),
            },
        )
