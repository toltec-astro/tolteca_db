"""Graph asset for parallel quartet processing.

Modern graph-backed asset implementing fan-out/fan-in pattern:
1. Query quartet metadata (entry point)
2. Fan-out to 11 interfaces using DynamicOut
3. Process each interface in parallel with .map()
4. Collect results and aggregate with .collect()
"""

# CRITICAL: Cannot use `from __future__ import annotations` with Dagster
# Dagster's runtime type validation requires actual type objects, not strings
# See: https://github.com/dagster-io/dagster/issues/28342

from datetime import datetime

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    Config,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    graph_asset,
    op,
)

from .partitions import quartet_partitions

__all__ = ["process_quartet", "association_groups"]


class QuartetMetadata(Config):
    """Configuration for quartet processing.

    Parsed from partition key format: "ics-{obsnum}-{subobsnum}-{scannum}"
    """

    master: str
    obsnum: int
    subobsnum: int
    scannum: int


class InterfaceData(Config):
    """Data for a single interface."""

    interface: str
    roach_index: int
    filename: str
    valid: int
    processed_at: str


class QuartetResult(Config):
    """Results from processing all interfaces in quartet."""

    quartet_key: str
    processed_interfaces: int
    success_count: int
    failure_count: int
    interface_results: list[dict]
    completed_at: str


@op(
    name="query_quartet",
    description="Query quartet metadata from toltec_db",
    required_resource_keys={"toltec_db"},
)
def query_quartet(context: OpExecutionContext) -> QuartetMetadata:
    """
    Query quartet metadata from toltec_db.

    Parameters
    ----------
    context : OpExecutionContext
        Execution context with partition_key

    Returns
    -------
    QuartetMetadata
        Quartet metadata parsed from partition key

    Examples
    --------
    >>> # Partition key: "ics-18846-0-0"
    >>> metadata = query_quartet(context)
    >>> metadata.master  # "ics"
    >>> metadata.obsnum  # 18846
    """
    partition_key = context.partition_key
    context.log.info(f"Processing quartet: {partition_key}")

    # Parse partition key: "ics-{obsnum}-{subobsnum}-{scannum}"
    parts = partition_key.split("-")
    if len(parts) != 4:
        raise ValueError(f"Invalid partition key format: {partition_key}")

    master, obsnum_str, subobsnum_str, scannum_str = parts

    return QuartetMetadata(
        master=master,
        obsnum=int(obsnum_str),
        subobsnum=int(subobsnum_str),
        scannum=int(scannum_str),
    )


@op(
    name="fan_out_interfaces",
    description="Fan-out to individual interfaces for parallel processing",
    required_resource_keys={"toltec_db", "validation"},
    out=DynamicOut(),
)
def fan_out_interfaces(context: OpExecutionContext, metadata: QuartetMetadata):
    """
    Fan-out to all enabled interfaces in quartet.

    Queries toltec_db for all interfaces in this quartet and emits
    DynamicOutput for each enabled interface.

    Parameters
    ----------
    context : OpExecutionContext
        Execution context
    metadata : QuartetMetadata
        Quartet metadata from query_quartet op

    Yields
    ------
    DynamicOutput[InterfaceData]
        One output per enabled interface for parallel processing

    Examples
    --------
    >>> # For quartet "ics-18846-0-0":
    >>> outputs = fan_out_interfaces(context, metadata)
    >>> # Yields 11 DynamicOutputs (one per enabled interface)
    >>> # Each has mapping_key="toltec0", "toltec1", ..., "toltec12"
    """
    from .helpers import query_quartet_status

    toltec_db = context.resources.toltec_db
    validation = context.resources.validation

    with toltec_db.get_session() as session:
        quartet_status = query_quartet_status(
            master=metadata.master,
            obsnum=metadata.obsnum,
            subobsnum=metadata.subobsnum,
            scannum=metadata.scannum,
            session=session,
        )

    if not quartet_status:
        raise RuntimeError(
            f"Quartet not found: {metadata.master}-{metadata.obsnum}-"
            f"{metadata.subobsnum}-{metadata.scannum}"
        )

    disabled_interfaces = validation.disabled_interfaces
    enabled_count = 0

    for interface_status in quartet_status:
        roach_index = interface_status["roach_index"]

        # Skip disabled interfaces
        if roach_index in disabled_interfaces:
            context.log.info(f"Skipping disabled interface: toltec{roach_index}")
            continue

        interface = f"toltec{roach_index}"
        enabled_count += 1

        interface_data = InterfaceData(
            interface=interface,
            roach_index=roach_index,
            filename=interface_status.get("filename", ""),
            valid=interface_status.get("valid", 0),
            processed_at=datetime.utcnow().isoformat(),
        )

        context.log.info(f"Fanning out to {interface} (valid={interface_data.valid})")

        yield DynamicOutput(
            value=interface_data,
            mapping_key=interface,
        )

    context.log.info(f"Fanned out to {enabled_count} enabled interfaces")


@op(
    name="process_interface",
    description="Process a single interface",
    required_resource_keys={"toltec_db", "tolteca_db", "location"},
)
def process_interface(
    context: OpExecutionContext,
    metadata: QuartetMetadata,
    interface_data: InterfaceData,
) -> dict:
    """
    Process a single interface.

    This is the core processing logic that runs in parallel for each
    interface. Reuses logic from per-interface experiment but within
    single Dagster run.

    Parameters
    ----------
    context : OpExecutionContext
        Execution context
    metadata : QuartetMetadata
        Quartet metadata
    interface_data : InterfaceData
        Interface-specific data

    Returns
    -------
    dict
        Processing result with status and statistics

    Examples
    --------
    >>> result = process_interface(context, metadata, interface_data)
    >>> result["interface"]  # "toltec0"
    >>> result["status"]     # "success"
    >>> result["rows_processed"]  # 4096
    """
    from .helpers import process_interface_data

    context.log.info(f"Processing {interface_data.interface}...")

    try:
        toltec_db = context.resources.toltec_db
        tolteca_db = context.resources.tolteca_db
        location = context.resources.location

        with toltec_db.get_session() as session:
            result = process_interface_data(
                master=metadata.master,
                obsnum=metadata.obsnum,
                subobsnum=metadata.subobsnum,
                scannum=metadata.scannum,
                roach_index=interface_data.roach_index,
                session=session,
                tolteca_db=tolteca_db,
                location=location,
            )

        context.log.info(
            f"✓ {interface_data.interface}: {result.get('rows_processed', 0)} rows"
        )

        return {
            "interface": interface_data.interface,
            "roach_index": interface_data.roach_index,
            "status": "success",
            "rows_processed": result.get("rows_processed", 0),
            "duration_seconds": result.get("duration_seconds", 0),
            "data_prod_pk": result.get("data_prod_pk"),  # Forward data_prod_pk for tel file processing
            "error": None,
        }

    except Exception as e:
        context.log.error(f"✗ {interface_data.interface}: {e}")
        # Re-raise the exception to fail the step and the run
        # This ensures database errors and other critical failures are visible
        raise RuntimeError(f"Failed to process {interface_data.interface}: {e}") from e


@op(
    name="collect_results",
    description="Collect and aggregate results from all interfaces",
    required_resource_keys={"tolteca_db", "location"},
)
def collect_results(
    context: OpExecutionContext,
    metadata: QuartetMetadata,
    interface_results: list[dict],
) -> QuartetResult:
    """
    Collect and aggregate results from all interface processing.

    Parameters
    ----------
    context : OpExecutionContext
        Execution context
    metadata : QuartetMetadata
        Quartet metadata
    interface_results : list[dict]
        Results from process_interface mapped ops

    Returns
    -------
    QuartetResult
        Aggregated results for entire quartet

    Examples
    --------
    >>> result = collect_results(context, metadata, interface_results)
    >>> result.quartet_key  # "ics-18846-0-0"
    >>> result.success_count  # 11
    >>> result.processed_interfaces  # 11
    """
    from tolteca_db.utils.uid import make_raw_obs_uid

    quartet_key = make_raw_obs_uid(
        metadata.master, metadata.obsnum, metadata.subobsnum, metadata.scannum
    )

    success_count = sum(1 for r in interface_results if r["status"] == "success")
    failure_count = len(interface_results) - success_count

    total_rows = sum(r["rows_processed"] for r in interface_results)
    total_duration = sum(r["duration_seconds"] for r in interface_results)

    context.log.info(
        f"Quartet {quartet_key}: {success_count}/{len(interface_results)} "
        f"interfaces succeeded"
    )
    context.log.info(f"Total rows processed: {total_rows}")
    context.log.info(f"Total processing time: {total_duration:.2f}s")

    if failure_count > 0:
        failures = [r for r in interface_results if r["status"] == "failure"]
        context.log.error(f"Failed interfaces: {[r['interface'] for r in failures]}")
        for failure in failures:
            context.log.error(f"  {failure['interface']}: {failure['error']}")

        # Fail the run if any interfaces failed
        # This ensures errors are surfaced rather than hidden in success status
        raise RuntimeError(
            f"Quartet processing failed: {failure_count}/{len(interface_results)} "
            f"interfaces failed. Failed: {[r['interface'] for r in failures]}"
        )

    # Add tel file as additional source to DataProd
    # Get data_prod_pk from first successful interface result
    data_prod_pk = None
    for result in interface_results:
        if result["status"] == "success" and "data_prod_pk" in result:
            data_prod_pk = int(result["data_prod_pk"])
            break
    
    if data_prod_pk:
        from .helpers import add_tel_file_source
        
        tolteca_db = context.resources.tolteca_db
        location = context.resources.location
        
        tel_result = add_tel_file_source(
            master=metadata.master,
            obsnum=metadata.obsnum,
            subobsnum=metadata.subobsnum,
            scannum=metadata.scannum,
            data_prod_pk=data_prod_pk,
            tolteca_db=tolteca_db,
            location=location,
        )
        
        if tel_result["added"]:
            context.log.info(f"✓ Added tel file: {tel_result['source_uri']}")
        elif tel_result["status"] == "tel_file_not_found":
            context.log.warning(f"⚠ Tel file not found for quartet {quartet_key}")
        elif tel_result["status"] == "already_exists":
            context.log.info(f"Tel file already exists: {tel_result['source_uri']}")
        else:
            context.log.warning(f"Tel file not added: {tel_result['status']}")

    return QuartetResult(
        quartet_key=quartet_key,
        processed_interfaces=len(interface_results),
        success_count=success_count,
        failure_count=failure_count,
        interface_results=interface_results,
        completed_at=datetime.utcnow().isoformat(),
    )


@graph_asset(
    name="process_quartet",
    description="Process all interfaces in a quartet in parallel",
    partitions_def=quartet_partitions,
)
def process_quartet():
    """
    Graph asset for parallel quartet processing.

    Architecture
    ------------
    Modern fan-out/fan-in pattern within single Dagster run:

    1. **Entry**: Query quartet metadata from partition key
    2. **Fan-out**: Emit DynamicOutput for each enabled interface
    3. **Parallel**: Process each interface using .map()
    4. **Fan-in**: Collect and aggregate results using .collect()

    This eliminates per-interface job overhead while maintaining
    parallel processing benefits.

    Returns
    -------
    QuartetResult
        Aggregated results from all interface processing

    Examples
    --------
    Asset materializes automatically when sensor creates RunRequest:

    >>> # Sensor detects complete quartet "ics-18846-0-0"
    >>> # Creates RunRequest with partition_key="ics-18846-0-0"
    >>> # Dagster materializes process_quartet asset
    >>> # Result: All 11 interfaces processed in parallel within single run

    Manual materialization for testing:

    >>> # dagster asset materialize -a process_quartet \\
    >>> #   --partition ics-18846-0-0

    Performance
    -----------
    - Single Dagster run (not 11+ separate runs)
    - Parallel interface processing (11 threads)
    - Reduced overhead (no automation conditions)
    - Simpler debugging (single run to inspect)
    """
    # 1. Entry point: Query quartet metadata
    metadata = query_quartet()

    # 2. Fan-out: Create DynamicOutput for each interface
    interface_data = fan_out_interfaces(metadata)

    # 3. Parallel: Process each interface (11 in parallel)
    interface_results = interface_data.map(
        lambda iface_data: process_interface(metadata, iface_data)
    )

    # 4. Fan-in: Collect and aggregate all results
    quartet_result = collect_results(metadata, interface_results.collect())

    return quartet_result


# Association Generation Asset
# Uses time-based partitions (daily) rather than quartet partitions
# because associations span multiple observations


from dagster import asset  # noqa: E402


@asset(
    description="Generate data product associations incrementally",
    required_resource_keys={"tolteca_db"},
    deps=[process_quartet],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def association_groups(context: AssetExecutionContext) -> dict:
    """
    Generate associations between data products.

    This asset creates provenance links in the data_prod_assoc table:
    - Raw obs uses calibration obs (dpa_raw_obs_cal_obs)
    - Reduced obs derived from raw obs (dpa_reduced_obs_raw_obs)
    - Calibration group membership (dpa_cal_group_raw_obs)
    - Focus analysis groupings (dpa_focus_raw_obs)
    - Drive fit groupings (dpa_drivefit_raw_obs)

    Uses incremental processing: only processes ungrouped observations
    from the dataprod table.

    Architecture
    ------------
    1. Query all observations from dataprod table (optionally time-limited)
    2. Load association state (which obs are already grouped)
    3. Filter to ungrouped observations only (incremental)
    4. Run collators to generate associations
    5. Update state and commit

    Performance: O(n_new + g) where n_new = ungrouped obs, g = existing groups
    vs O(n²) for full rescan.

    Parameters
    ----------
    context : AssetExecutionContext
        Execution context

    Returns
    -------
    dict
        Statistics with keys:
        - observations_scanned: Total obs loaded from dataprod
        - observations_already_grouped: Skipped (already in groups)
        - observations_processed: Actually processed (new obs)
        - groups_created: New groups created
        - groups_updated: Existing groups with new members
        - associations_created: Association entries created

    Examples
    --------
    Materialize to process new observations:

    >>> # dagster asset materialize -a association_groups

    After process_quartet runs populate dataprod:

    >>> # Sensor detects complete quartet → process_quartet → dataprod populated
    >>> # Manually materialize or schedule to run periodically (hourly/daily)
    >>> # Incremental processing skips already-grouped observations
    """
    from sqlalchemy import select
    from tolteca_db.associations import (
        AssociationGenerator,
        AssociationState,
        DatabaseBackend,
    )
    from tolteca_db.models.orm import DataProd, DataProdType

    context.log.info("Generating associations for all observations")

    # Get tolteca_db resource for session management
    tolteca_db = context.resources.tolteca_db

    with tolteca_db.get_session() as session:
        # Query all raw observations from dataprod
        # Incremental processing will skip already-grouped observations

        # Get dp_raw_obs type PK
        dp_type_stmt = select(DataProdType).where(DataProdType.label == "dp_raw_obs")
        dp_raw_obs_type = session.execute(dp_type_stmt).scalar_one()

        # Query all observations, ordered by creation time
        # Optional: Add time limit (e.g., last 30 days) if dataprod grows large
        obs_stmt = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == dp_raw_obs_type.pk)
            .order_by(DataProd.created_at)
        )

        observations = list(session.execute(obs_stmt).scalars().all())

        context.log.info(f"Found {len(observations)} observations in dataprod")

        if not observations:
            context.log.warning("No observations found in dataprod")
            return {
                "observations_scanned": 0,
                "observations_already_grouped": 0,
                "observations_processed": 0,
                "groups_created": 0,
                "groups_updated": 0,
                "associations_created": 0,
            }

        # Initialize state tracking with database backend
        # DatabaseBackend queries data_prod_assoc table to load grouped obs
        state = AssociationState(DatabaseBackend(session))

        # Create generator with state for incremental processing
        generator = AssociationGenerator(session, state=state)

        # Generate associations from the date-filtered batch
        # Only processes observations that aren't already grouped
        stats = generator.generate_from_batch(
            observations=observations,
            incremental=True,  # Skip already-grouped observations
            commit=True,  # Persist to database
        )

        # Log statistics
        context.log.info(
            f"Association generation complete:\n"
            f"  Scanned: {stats.observations_scanned} observations\n"
            f"  Already grouped: {stats.observations_already_grouped}\n"
            f"  Processed: {stats.observations_processed}\n"
            f"  Groups created: {stats.groups_created}\n"
            f"  Groups updated: {stats.groups_updated}\n"
            f"  Associations created: {stats.associations_created}"
        )

        # Return stats for Dagster UI
        return {
            "observations_scanned": stats.observations_scanned,
            "observations_already_grouped": stats.observations_already_grouped,
            "observations_processed": stats.observations_processed,
            "groups_created": stats.groups_created,
            "groups_updated": stats.groups_updated,
            "associations_created": stats.associations_created,
        }
