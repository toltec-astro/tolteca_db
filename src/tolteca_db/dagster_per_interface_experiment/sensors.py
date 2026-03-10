"""Sensor definitions for Dagster pipeline.

Implements toltec_db_sync_sensor for detecting new observations
and triggering asset materialization.

References
----------
- Dagster Sensors: https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

__all__ = [
    "sync_with_toltec_db",
    # quartet_completion_sensor removed - replaced by eager().without(~any_deps_missing())
]

from dagster import (
    DefaultSensorStatus,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from .assets import (
    raw_obs_product,
    # quartet_complete import removed - no longer needed by sensors
)
from tolteca_db.utils.uid import make_raw_obs_uid


@sensor(
    name="toltec_db_sync_sensor",
    target=[raw_obs_product],
    minimum_interval_seconds=5,  # 5 seconds for interface-level detection
    description="Detect per-interface validation in toltec_db and trigger processing",
    required_resource_keys={"toltec_db"},
    default_status=DefaultSensorStatus.RUNNING,  # Sensor enabled by default
)
def sync_with_toltec_db(context: SensorEvaluationContext):
    """
    Sensor that detects per-interface validation and creates 2D partitions.

    This sensor polls toltec_db every ~5 seconds for interface validation
    changes. Each observation has 13 interfaces (toltec0-12) that validate
    independently, creating a 2D partition space (quartet × interface).

    Parameters
    ----------
    context : SensorEvaluationContext
        Dagster sensor context with cursor for state tracking

    Yields
    ------
    RunRequest | SkipReason
        RunRequest for each new interface validation, or SkipReason if none

    Examples
    --------
    Sensor runs automatically every 5 seconds when Dagster daemon is running:
    >>> # dagster sensor list
    >>> # dagster sensor start toltec_db_sync_sensor

    Manual trigger for testing:
    >>> # dagster sensor test toltec_db_sync_sensor

    Notes
    -----
    - **2D Partitions**: quartet × interface (1-1 mapping to toltec_db rows)
    - **High-frequency polling**: 5-second intervals detect staggered validation
    - **Per-interface tracking**: Each interface gets its own partition
    - **Validation status**: Tags indicate Valid=0 (in progress) or Valid=1 (complete)
    - **Timeline order**: Quartet partitions added in observation sequence
    - **Idempotent**: Safe to re-run, won't create duplicate partitions

    Partition Structure:
    - Quartet: "toltec-1-123456-0-0" (master-obsnum-subobsnum-scannum)
    - Interface: "toltec0" through "toltec12" (13 interfaces)
    - 2D Key: {"quartet": "toltec-1-123456-0-0", "interface": "toltec5"}

    Query Capabilities:
    - All interfaces: --partition quartet=toltec-1-123456-0-0
    - Single interface: --partition quartet=toltec-1-123456-0-0 interface=toltec5
    - By tags: --tags master=toltec valid=1 array_name=a1100
    """
    from tolteca_db.dagster.helpers import query_toltec_db_since
    from tolteca_db.dagster.partitions import get_array_name_for_interface

    # Get last check timestamp from cursor (persistent state)
    last_check = context.cursor or "2024-01-01T00:00:00Z"
    last_check_dt = datetime.fromisoformat(last_check.replace("Z", "+00:00"))

    context.log.info(f"Checking for interface updates since {last_check}")

    # Query toltec_db for new/updated observations
    # Returns all interfaces (Valid=0 and Valid=1)
    toltec_db = context.resources.toltec_db
    with toltec_db.get_session() as session:
        # Debug: Check database connection and tables
        from sqlalchemy import text, inspect

        inspector = inspect(session.bind)
        tables = inspector.get_table_names()
        context.log.info(f"DEBUG: Available tables in database: {tables}")

        # Debug: Check if toltec table has any rows
        if "toltec" in tables:
            row_count = session.execute(
                text("SELECT COUNT(*) as count FROM toltec")
            ).fetchone()
            context.log.info(
                f"DEBUG: Total rows in toltec table: {row_count.count if row_count else 0}"
            )

            # Debug: Show sample of recent data
            sample_rows = session.execute(
                text("""
                SELECT ObsNum, SubObsNum, ScanNum, RoachIndex, Valid, Date, Time 
                FROM toltec 
                ORDER BY Date DESC, Time DESC 
                LIMIT 5
            """)
            ).fetchall()
            context.log.info(f"DEBUG: Sample rows from toltec table:")
            for row in sample_rows:
                context.log.info(
                    f"  ObsNum={row.ObsNum}, SubObsNum={row.SubObsNum}, ScanNum={row.ScanNum}, "
                    f"RoachIndex={row.RoachIndex}, Valid={row.Valid}, Date={row.Date}, Time={row.Time}"
                )

        context.log.info(
            f"DEBUG: Querying with since_date={last_check_dt.date().isoformat()}, "
            f"since_time={last_check_dt.time().isoformat()}"
        )

        new_obs = query_toltec_db_since(last_check_dt, session=session)

        context.log.info(
            f"DEBUG: query_toltec_db_since returned {len(new_obs) if new_obs else 0} observations"
        )

    if not new_obs:
        return SkipReason(f"No interface updates since {last_check}")

    # Separate valid and invalid interfaces for reporting
    valid_obs = [obs for obs in new_obs if obs.get("valid", 0) == 1]
    invalid_obs = [obs for obs in new_obs if obs.get("valid", 0) != 1]

    context.log.info(
        f"Found {len(new_obs)} interface updates "
        f"({len(valid_obs)} valid, {len(invalid_obs)} in-progress)"
    )

    # Build 2D partition keys and run requests
    # Each observation can have multiple interfaces updated
    quartet_keys_seen = set()
    interface_partitions_seen = set()  # Track (quartet, interface) pairs
    run_requests = []

    for obs in new_obs:
        # Extract observation quartet identifier
        quartet_key = make_raw_obs_uid(
            obs["master"],
            obs["obsnum"],
            obs["subobsnum"],
            obs["scannum"],
        )

        # Get interface-specific information
        roach_index = obs.get("roach_index", 0)  # 0-12
        interface = f"toltec{roach_index}"

        # Create unique key for this interface partition to avoid duplicates
        # This prevents creating multiple runs for the same partition in a single sensor tick
        interface_partition_key = (quartet_key, interface)
        if interface_partition_key in interface_partitions_seen:
            continue  # Skip duplicate
        interface_partitions_seen.add(interface_partition_key)

        # Register quartet partition if first time seeing it
        if quartet_key not in quartet_keys_seen:
            context.instance.add_dynamic_partitions(
                partitions_def_name="quartet",
                partition_keys=[quartet_key],
            )
            quartet_keys_seen.add(quartet_key)

        array_name = get_array_name_for_interface(interface)
        valid = obs.get("valid", 0)  # 0=in progress, 1=complete

        # 2D partition key format - must use MultiPartitionKey for 2D partitions
        # Dimension names: quartet, quartet_interface (alphabetically sorted)
        partition_key = MultiPartitionKey(
            {
                "quartet": quartet_key,
                "quartet_interface": interface,
            }
        )

        # Rich tags for filtering and tracking
        obs_timestamp = obs.get("timestamp", datetime.now(timezone.utc))
        tags = {
            # Observation identifiers
            "master": obs["master"],
            "obsnum": str(obs["obsnum"]),
            "subobsnum": str(obs["subobsnum"]),
            "scannum": str(obs["scannum"]),
            # Interface information
            "interface": interface,
            "roach_index": str(roach_index),
            "array_name": array_name,
            # Validation status
            "valid": str(valid),
            "validation_status": "complete" if valid == 1 else "in_progress",
            # Timing
            "obs_date": obs_timestamp.date().isoformat(),
            "obs_timestamp": obs_timestamp.isoformat(),
            # Additional metadata
            "obs_type": obs.get("obs_type", "unknown"),
            "source": "toltec_db_sync",
        }

        # Only create runs for valid interfaces (Valid=1)
        # Invalid interfaces still get partition keys created, but runs are skipped
        if valid == 1:
            run_requests.append(
                RunRequest(
                    partition_key=partition_key,
                    tags=tags,
                )
            )

        status_icon = "✓" if valid == 1 else "⏳"
        context.log.info(
            f"  {status_icon} {quartet_key} / {interface} ({array_name}) valid={valid}"
        )

    # Update cursor to latest observation timestamp
    if new_obs:
        latest_timestamp = max(
            obs.get("timestamp", datetime.now(timezone.utc)) for obs in new_obs
        )
        context.update_cursor(latest_timestamp.isoformat())
        context.log.info(f"Updated cursor to {latest_timestamp.isoformat()}")

    # Quartet completion is now handled by a separate sensor (quartet_completion_sensor)
    # This sensor only handles per-interface triggering

    context.log.info(
        f"Created {len(run_requests)} interface runs from {len(quartet_keys_seen)} observations"
    )

    return run_requests


# quartet_completion_sensor removed - replaced by declarative automation
#
# The quartet_complete asset now uses customized AutomationCondition.eager() which:
# 1. Uses eager().without(~any_deps_missing()) to allow execution with missing deps
# 2. Allows missing upstream deps (disabled interfaces unknown in advance)
# 3. The asset execution logic checks timeout and determines enabled interfaces dynamically
# 4. Eliminates the race condition by waiting for upstream deps (but allows missing ones)
# 5. More responsive than 30s polling - executes when upstream deps ready
#
# This follows Dagster's recommended pattern for aggregation assets where the complete
# set of dependencies is not known in advance and must be determined dynamically.
