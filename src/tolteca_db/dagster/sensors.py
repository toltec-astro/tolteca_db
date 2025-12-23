"""Sensor for detecting complete quartets in toltec_db.

Modern sensor implementation that:
1. Polls toltec_db for complete quartets (all enabled interfaces Valid=1)
2. Creates 1D dynamic partitions for each complete quartet
3. Triggers single run per quartet for fan-out processing
"""

# CRITICAL: Cannot use `from __future__ import annotations` with Dagster
# Dagster's runtime type validation requires actual type objects, not strings

from datetime import datetime, timezone
from typing import Dict

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from tolteca_db.utils.uid import make_raw_obs_uid

from .assets import process_quartet

__all__ = ["quartet_sensor"]


class QuartetValidationTracker:
    """Track validation state and timeout for quartet completion detection.

    Handles dynamically disabled interfaces using timeout-based logic:
    - Timer starts when first interface becomes Valid=1
    - Timer resets on each new Valid=0 → Valid=1 transition
    - Quartet is complete when timer expires with no new transitions

    This allows detection of:
    - Dynamically disabled interfaces (won't become valid)
    - Stuck data acquisition (timeout prevents indefinite waiting)
    - Variable number of active interfaces per observation
    """

    def __init__(self, validation_timeout_seconds: float = 30.0):
        """Initialize tracker.

        Parameters
        ----------
        validation_timeout_seconds : float
            Timeout in seconds for no new Valid=1 transitions
        """
        self.validation_timeout_seconds = validation_timeout_seconds
        # Track state per quartet: {quartet_key: {"first_valid_time": ISO string, "last_valid_time": ISO string, "valid_count": int}}
        self.quartet_states: Dict[str, Dict] = {}

    def update(
        self, quartet_key: str, valid_count: int, current_time: datetime
    ) -> None:
        """Update validation state for quartet.

        Parameters
        ----------
        quartet_key : str
            Quartet identifier
        valid_count : int
            Current number of Valid=1 interfaces
        current_time : datetime
            Current timestamp
        """
        if quartet_key not in self.quartet_states:
            # First time seeing this quartet with valid interfaces
            if valid_count > 0:
                self.quartet_states[quartet_key] = {
                    "first_valid_time": current_time.isoformat(),
                    "last_valid_time": current_time.isoformat(),
                    "valid_count": valid_count,
                }
        else:
            state = self.quartet_states[quartet_key]
            # Check if valid_count increased (new Valid=0 → Valid=1 transition)
            if valid_count > state["valid_count"]:
                # Reset timer - new interface became valid
                state["last_valid_time"] = current_time.isoformat()
                state["valid_count"] = valid_count

    def is_complete(
        self,
        quartet_key: str,
        valid_count: int,
        expected_count: int,
        current_time: datetime,
    ) -> tuple[bool, str]:
        """Check if quartet is complete.

        Parameters
        ----------
        quartet_key : str
            Quartet identifier
        valid_count : int
            Current number of Valid=1 interfaces
        expected_count : int
            Expected number of interfaces (max - disabled)
        current_time : datetime
            Current timestamp

        Returns
        -------
        tuple[bool, str]
            (is_complete, reason)
        """
        # Strategy 1: All expected interfaces are valid
        if valid_count == expected_count:
            return True, f"all {expected_count} interfaces valid"

        # Strategy 2: Timeout-based completion
        if quartet_key in self.quartet_states:
            state = self.quartet_states[quartet_key]
            last_transition = datetime.fromisoformat(state["last_valid_time"])
            time_since_last = (current_time - last_transition).total_seconds()

            if time_since_last >= self.validation_timeout_seconds:
                # Timeout expired - no new Valid=1 transitions
                return (
                    True,
                    f"timeout ({time_since_last:.1f}s since last Valid=1, "
                    f"{valid_count}/{expected_count} valid)",
                )

        return False, f"{valid_count}/{expected_count} valid, waiting for timeout"


@sensor(
    name="quartet_sensor",
    target=process_quartet,
    minimum_interval_seconds=10,  # Poll every 10 seconds for complete quartets
    description="Detect complete quartets in toltec_db and trigger processing",
    required_resource_keys={"toltec_db", "tolteca_db", "validation"},
    default_status=DefaultSensorStatus.RUNNING,
)
def quartet_sensor(context: SensorEvaluationContext):
    """
    Sensor that detects complete quartets in toltec_db.

    Strategy
    --------
    1. Query toltec_db for all recent quartets
    2. Group interfaces by quartet (obsnum, subobsnum, scannum)
    3. Check completion criteria:
       - All enabled interfaces have Valid=1
       - Account for disabled interfaces from validation config
    4. Create RunRequest for each complete quartet

    Completion Criteria
    -------------------
    A quartet is complete when all ENABLED interfaces have Valid=1:
    - Expected: 11 interfaces (13 total - 2 disabled)
    - Query only checks enabled interfaces
    - Disabled interfaces are ignored

    Parameters
    ----------
    context : SensorEvaluationContext
        Dagster sensor context with cursor for state tracking

    Yields
    ------
    RunRequest | SkipReason
        RunRequest for each complete quartet, or SkipReason if none

    Examples
    --------
    Sensor runs automatically every 10 seconds when Dagster daemon is running:
    >>> # dagster sensor list
    >>> # dagster sensor start quartet_sensor

    Manual trigger for testing:
    >>> # dagster sensor test quartet_sensor
    """
    from .helpers import query_toltec_db_since

    # Get state from cursor (persistent across sensor evaluations)
    # Cursor format: JSON with {"last_check": "ISO timestamp", "quartet_states": {...}, "completed_quartets": [...]}
    import json
    import os

    # Use same date range as batch ingestion for consistency
    # Priority: TOLTECA_SIMULATOR_DATE > TOLTECA_INGEST_START_DATE > default (7 days ago)
    default_start_date = os.getenv("TOLTECA_SIMULATOR_DATE") or os.getenv(
        "TOLTECA_INGEST_START_DATE"
    )
    if not default_start_date:
        # Fallback to 7 days ago (same as run_ingest_all.sh)
        from datetime import timedelta
        default_start_date = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    
    # Ensure ISO format with timezone
    if "T" not in default_start_date:
        default_start_date = f"{default_start_date}T00:00:00Z"
    elif not default_start_date.endswith("Z"):
        default_start_date = f"{default_start_date}Z"

    if context.cursor:
        try:
            cursor_data = json.loads(context.cursor)
            last_check = cursor_data.get("last_check", default_start_date)
            saved_states = cursor_data.get("quartet_states", {})
        except (json.JSONDecodeError, ValueError):
            # Backward compatibility: treat as plain timestamp string
            last_check = context.cursor
            saved_states = {}
    else:
        last_check = default_start_date
        saved_states = {}

    last_check_dt = datetime.fromisoformat(last_check.replace("Z", "+00:00"))
    context.log.info(
        f"Checking for complete quartets since {last_check}"
    )

    # Query toltec_db for new/updated observations
    toltec_db = context.resources.toltec_db
    tolteca_db = context.resources.tolteca_db
    validation = context.resources.validation

    # Helper function to check if quartet already ingested
    def data_prod_exists(quartet_key: str) -> bool:
        """Check if DataProd already exists for this quartet."""
        from tolteca_db.models import DataProd
        import sqlalchemy as sa
        
        # Parse quartet_key: "ics-17810-0-0" → master, obsnum, subobsnum, scannum
        parts = quartet_key.split('-')
        if len(parts) != 4:
            return False
        
        master = parts[0]
        try:
            obsnum = int(parts[1])
            subobsnum = int(parts[2])
            scannum = int(parts[3])
        except ValueError:
            return False
        
        # Query for matching DataProd in tolteca_db
        with tolteca_db.get_session() as session:
            result = session.query(DataProd).filter(
                sa.cast(
                    sa.func.json_extract(DataProd.meta, '$.master'),
                    sa.String
                ) == master,
                sa.cast(
                    sa.func.json_extract(DataProd.meta, '$.obsnum'),
                    sa.Integer
                ) == obsnum,
                sa.cast(
                    sa.func.json_extract(DataProd.meta, '$.subobsnum'),
                    sa.Integer
                ) == subobsnum,
                sa.cast(
                    sa.func.json_extract(DataProd.meta, '$.scannum'),
                    sa.Integer
                ) == scannum,
            ).first()
            
            return result is not None

    with toltec_db.get_session() as session:
        new_obs = query_toltec_db_since(last_check_dt, session=session)

    if not new_obs:
        return SkipReason(f"No observations since {last_check}")

    # Group observations by quartet
    quartets = {}
    for obs in new_obs:
        master = obs["master"]
        obsnum = obs["obsnum"]
        subobsnum = obs["subobsnum"]
        scannum = obs["scannum"]
        roach_index = obs.get("roach_index", 0)
        valid = obs.get("valid", 0)

        quartet_key = make_raw_obs_uid(master, obsnum, subobsnum, scannum)

        if quartet_key not in quartets:
            quartets[quartet_key] = {
                "master": master,
                "obsnum": obsnum,
                "subobsnum": subobsnum,
                "scannum": scannum,
                "interfaces": {},
                "timestamp": obs.get("timestamp", datetime.now(timezone.utc)),
            }

        interface = f"toltec{roach_index}"
        quartets[quartet_key]["interfaces"][interface] = {
            "roach_index": roach_index,
            "valid": valid,
            "filename": obs.get("filename", ""),
        }

    # Initialize validation tracker with saved state from cursor
    tracker = QuartetValidationTracker(
        validation_timeout_seconds=getattr(
            validation, "validation_timeout_seconds", 30.0
        )
    )
    # Restore saved states
    tracker.quartet_states = saved_states

    # Check completion for each quartet using timeout-based logic
    run_requests = []
    disabled_interfaces = validation.disabled_interfaces
    expected_count = validation.max_interface_count - len(disabled_interfaces)
    current_time = datetime.now(timezone.utc)
    
    # Batch size limit to prevent timeout (configurable via env var)
    batch_size = int(os.getenv("QUARTET_SENSOR_BATCH_SIZE", "50"))
    
    # Track latest quartet timestamp to advance cursor
    latest_quartet_time = last_check_dt
    
    # Track which quartets are already ingested (to clean up quartet_states)
    already_ingested_keys = set()

    for quartet_key, quartet_data in quartets.items():
        interfaces = quartet_data["interfaces"]

        # Filter out disabled interfaces
        enabled_interfaces = {
            iface: data
            for iface, data in interfaces.items()
            if data["roach_index"] not in disabled_interfaces
        }

        # Count valid interfaces
        valid_count = sum(
            1 for data in enabled_interfaces.values() if data["valid"] == 1
        )

        # Update tracker with current state
        tracker.update(quartet_key, valid_count, current_time)

        context.log.info(
            f"{quartet_key}: {valid_count}/{expected_count} enabled interfaces valid "
            f"({len(disabled_interfaces)} disabled)"
        )

        # Check if quartet is complete (all valid OR timeout)
        is_complete, reason = tracker.is_complete(
            quartet_key, valid_count, expected_count, current_time
        )

        if is_complete:
            # Update latest timestamp for cursor advancement
            obs_timestamp = quartet_data["timestamp"]
            # Ensure timezone-aware comparison
            if obs_timestamp.tzinfo is None:
                obs_timestamp = obs_timestamp.replace(tzinfo=timezone.utc)
            if latest_quartet_time.tzinfo is None:
                latest_quartet_time = latest_quartet_time.replace(tzinfo=timezone.utc)
            
            if obs_timestamp > latest_quartet_time:
                latest_quartet_time = obs_timestamp
            
            # Check if already ingested (duplicate detection)
            if data_prod_exists(quartet_key):
                context.log.info(
                    f"⏭️  Quartet {quartet_key} already ingested, skipping"
                )
                # Track this so we can clean up its state from cursor
                already_ingested_keys.add(quartet_key)
                continue

            # Register quartet partition if first time seeing it
            context.instance.add_dynamic_partitions(
                partitions_def_name="quartet",
                partition_keys=[quartet_key],
            )

            # Create rich tags for partition-based queries and backfill
            obs_timestamp = quartet_data["timestamp"]
            tags = {
                "master": quartet_data["master"],
                "obsnum": str(quartet_data["obsnum"]),
                "subobsnum": str(quartet_data["subobsnum"]),
                "scannum": str(quartet_data["scannum"]),
                "valid_count": str(valid_count),
                "expected_count": str(expected_count),
                "completion_reason": reason,
                # Time-based tags for backfill queries
                "obs_date": obs_timestamp.date().isoformat(),
                "obs_timestamp": obs_timestamp.isoformat(),
                "obs_year": str(obs_timestamp.year),
                "obs_month": f"{obs_timestamp.year}-{obs_timestamp.month:02d}",
            }

            run_requests.append(
                RunRequest(
                    partition_key=quartet_key,
                    run_key=f"quartet_{quartet_key}_{quartet_data['timestamp'].isoformat()}",
                    tags=tags,
                )
            )

            context.log.info(
                f"✓ Quartet {quartet_key} complete ({reason})! Creating run."
            )
            
            # BATCH SIZE LIMIT: Stop processing when batch is full
            if len(run_requests) >= batch_size:
                context.log.info(
                    f"Reached batch size limit ({batch_size}), will continue in next tick"
                )
                break
        else:
            context.log.info(f"⏳ Quartet {quartet_key}: {reason}")

    # Update cursor: advance last_check and save incomplete quartet states
    if quartets:
        # Advance last_check to latest quartet timestamp to avoid re-querying old data
        new_last_check = latest_quartet_time.isoformat().replace("+00:00", "Z")
        
        # Only keep states for incomplete quartets (remove completed and already-ingested ones)
        incomplete_states = {
            key: state 
            for key, state in tracker.quartet_states.items()
            if key in quartets 
            and key not in already_ingested_keys  # Remove already-ingested
            and not any(rr.partition_key == key for rr in run_requests)  # Remove completed
        }
        
        cursor_data = {
            "last_check": new_last_check,
            "quartet_states": incomplete_states,
        }
        import json

        context.update_cursor(json.dumps(cursor_data))
        context.log.info(
            f"Advanced cursor to {new_last_check}, tracking {len(incomplete_states)} incomplete quartets"
        )

    if run_requests:
        context.log.info(
            f"Created {len(run_requests)} run requests for complete quartets"
        )
        return run_requests
    else:
        return SkipReason("No complete quartets found")
