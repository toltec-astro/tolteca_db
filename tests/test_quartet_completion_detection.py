"""Test quartet completion detection in sensor.

This test verifies that the sensor properly detects when a quartet is complete
and triggers the quartet_complete asset.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dagster import (
    DagsterInstance,
    build_sensor_context,
)
from sqlalchemy import text

from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
from tolteca_db.dagster.resources import ToltecDBResource
from tolteca_db.dagster.sensors import sync_with_toltec_db


@pytest.fixture
def toltec_db_with_complete_quartet(tmp_path):
    """
    Create a test toltec_db with a complete quartet.
    
    Creates a quartet with all 13 interfaces valid, with last Valid=1
    transition more than 30 seconds ago (past timeout threshold).
    """
    db_path = tmp_path / "test_toltec_complete.db"
    resource = ToltecDBResource(database_url=f"sqlite:///{db_path}")
    
    # Create tables
    with resource.get_session() as session:
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT UNIQUE NOT NULL
            )
        """))
        
        session.execute(text("""
            CREATE TABLE toltec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Master INTEGER NOT NULL,
                ObsNum INTEGER NOT NULL,
                SubObsNum INTEGER NOT NULL,
                ScanNum INTEGER NOT NULL,
                RoachIndex INTEGER NOT NULL,
                Valid INTEGER NOT NULL DEFAULT 0,
                Date TEXT,
                Time TEXT,
                Filename TEXT,
                ObsType TEXT DEFAULT 'Science',
                FOREIGN KEY (Master) REFERENCES master(id)
            )
        """))
        
        session.execute(text("""
            CREATE TABLE interface_file (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                toltec_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                FOREIGN KEY (toltec_id) REFERENCES toltec(id)
            )
        """))
        
        # Insert master
        session.execute(text("""
            INSERT INTO master (id, label) VALUES (1, 'toltec')
        """))
        
        # Insert complete quartet - all 13 interfaces valid
        # Last Valid=1 transition was 45 seconds ago (past 30s timeout)
        now = datetime.now(timezone.utc)
        first_valid = now - timedelta(seconds=60)  # First interface 60s ago
        last_valid = now - timedelta(seconds=45)   # Last interface 45s ago
        
        for roach_index in range(13):
            # Stagger the Valid=1 timestamps across the 15-second window
            timestamp = first_valid + timedelta(seconds=(roach_index * 15 / 12))
            date_str = timestamp.date().isoformat()
            time_str = timestamp.time().isoformat()
            
            session.execute(text("""
                INSERT INTO toltec (
                    Master, ObsNum, SubObsNum, ScanNum, RoachIndex,
                    Valid, Date, Time, Filename, ObsType
                ) VALUES (
                    1, 123456, 0, 0, :roach_index,
                    1, :date, :time, :filename, 'Science'
                )
            """), {
                "roach_index": roach_index,
                "date": date_str,
                "time": time_str,
                "filename": f"toltec{roach_index}_123456_000_0000_Science.nc",
            })
        
        session.commit()
    
    return resource


@pytest.fixture
def toltec_db_with_incomplete_quartet(tmp_path):
    """
    Create a test toltec_db with an incomplete quartet.
    
    Creates a quartet with only 8/13 interfaces valid, with last Valid=1
    transition less than 30 seconds ago (before timeout threshold).
    """
    db_path = tmp_path / "test_toltec_incomplete.db"
    resource = ToltecDBResource(database_url=f"sqlite:///{db_path}")
    
    # Create tables
    with resource.get_session() as session:
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT UNIQUE NOT NULL
            )
        """))
        
        session.execute(text("""
            CREATE TABLE toltec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Master INTEGER NOT NULL,
                ObsNum INTEGER NOT NULL,
                SubObsNum INTEGER NOT NULL,
                ScanNum INTEGER NOT NULL,
                RoachIndex INTEGER NOT NULL,
                Valid INTEGER NOT NULL DEFAULT 0,
                Date TEXT,
                Time TEXT,
                Filename TEXT,
                ObsType TEXT DEFAULT 'Science',
                FOREIGN KEY (Master) REFERENCES master(id)
            )
        """))
        
        session.execute(text("""
            CREATE TABLE interface_file (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                toltec_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                FOREIGN KEY (toltec_id) REFERENCES toltec(id)
            )
        """))
        
        # Insert master
        session.execute(text("""
            INSERT INTO master (id, label) VALUES (1, 'toltec')
        """))
        
        # Insert incomplete quartet - only 8/13 interfaces valid
        # Last Valid=1 transition was 10 seconds ago (before 30s timeout)
        now = datetime.now(timezone.utc)
        last_valid = now - timedelta(seconds=10)
        
        for roach_index in range(13):
            # Only first 8 interfaces are valid
            valid = 1 if roach_index < 8 else 0
            timestamp = last_valid if valid else now
            date_str = timestamp.date().isoformat()
            time_str = timestamp.time().isoformat()
            
            session.execute(text("""
                INSERT INTO toltec (
                    Master, ObsNum, SubObsNum, ScanNum, RoachIndex,
                    Valid, Date, Time, Filename, ObsType
                ) VALUES (
                    1, 123456, 0, 0, :roach_index,
                    :valid, :date, :time, :filename, 'Science'
                )
            """), {
                "roach_index": roach_index,
                "valid": valid,
                "date": date_str,
                "time": time_str,
                "filename": f"toltec{roach_index}_123456_000_0000_Science.nc",
            })
        
        session.commit()
    
    return resource


@pytest.fixture
def toltec_db_with_new_quartet(tmp_path):
    """
    Create a test toltec_db with an old quartet and a newer quartet.
    
    When a newer quartet is detected, the old quartet is definitively complete
    regardless of timeout or valid count.
    """
    db_path = tmp_path / "test_toltec_new_quartet.db"
    resource = ToltecDBResource(database_url=f"sqlite:///{db_path}")
    
    # Create tables
    with resource.get_session() as session:
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT UNIQUE NOT NULL
            )
        """))
        
        session.execute(text("""
            CREATE TABLE toltec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Master INTEGER NOT NULL,
                ObsNum INTEGER NOT NULL,
                SubObsNum INTEGER NOT NULL,
                ScanNum INTEGER NOT NULL,
                RoachIndex INTEGER NOT NULL,
                Valid INTEGER NOT NULL DEFAULT 0,
                Date TEXT,
                Time TEXT,
                Filename TEXT,
                ObsType TEXT DEFAULT 'Science',
                FOREIGN KEY (Master) REFERENCES master(id)
            )
        """))
        
        session.execute(text("""
            CREATE TABLE interface_file (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                toltec_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                FOREIGN KEY (toltec_id) REFERENCES toltec(id)
            )
        """))
        
        # Insert master
        session.execute(text("""
            INSERT INTO master (id, label) VALUES (1, 'toltec')
        """))
        
        # Insert old quartet with only 8/13 interfaces valid
        now = datetime.now(timezone.utc)
        old_time = now - timedelta(minutes=5)
        
        for roach_index in range(13):
            valid = 1 if roach_index < 8 else 0
            date_str = old_time.date().isoformat()
            time_str = old_time.time().isoformat()
            
            session.execute(text("""
                INSERT INTO toltec (
                    Master, ObsNum, SubObsNum, ScanNum, RoachIndex,
                    Valid, Date, Time, Filename, ObsType
                ) VALUES (
                    1, 123456, 0, 0, :roach_index,
                    :valid, :date, :time, :filename, 'Science'
                )
            """), {
                "roach_index": roach_index,
                "valid": valid,
                "date": date_str,
                "time": time_str,
                "filename": f"toltec{roach_index}_123456_000_0000_Science.nc",
            })
        
        # Insert newer quartet (scannum=1)
        new_time = now - timedelta(seconds=10)
        
        for roach_index in range(3):  # Only 3 interfaces so far
            date_str = new_time.date().isoformat()
            time_str = new_time.time().isoformat()
            
            session.execute(text("""
                INSERT INTO toltec (
                    Master, ObsNum, SubObsNum, ScanNum, RoachIndex,
                    Valid, Date, Time, Filename, ObsType
                ) VALUES (
                    1, 123456, 0, 1, :roach_index,
                    1, :date, :time, :filename, 'Science'
                )
            """), {
                "roach_index": roach_index,
                "date": date_str,
                "time": time_str,
                "filename": f"toltec{roach_index}_123456_000_0001_Science.nc",
            })
        
        session.commit()
    
    return resource


def test_query_quartet_status_complete(toltec_db_with_complete_quartet):
    """Test that query_toltec_db_quartet_status detects complete quartet."""
    with toltec_db_with_complete_quartet.get_session() as session:
        status = query_toltec_db_quartet_status(
            "toltec", 123456, 0, 0, session=session
        )
    
    assert status["valid_count"] == 13, "All 13 interfaces should be valid"
    assert len(status["valid_interfaces"]) == 13
    assert len(status["invalid_interfaces"]) == 0
    assert len(status["missing_interfaces"]) == 0
    assert status["time_since_last_valid"] >= 30.0, "Should be past 30s timeout"
    assert status["new_quartet_detected"] is False


def test_query_quartet_status_incomplete(toltec_db_with_incomplete_quartet):
    """Test that query_toltec_db_quartet_status detects incomplete quartet."""
    with toltec_db_with_incomplete_quartet.get_session() as session:
        status = query_toltec_db_quartet_status(
            "toltec", 123456, 0, 0, session=session
        )
    
    assert status["valid_count"] == 8, "Only 8 interfaces should be valid"
    assert len(status["valid_interfaces"]) == 8
    assert len(status["invalid_interfaces"]) == 5
    assert len(status["missing_interfaces"]) == 0
    assert status["time_since_last_valid"] < 30.0, "Should be before 30s timeout"
    assert status["new_quartet_detected"] is False


def test_query_quartet_status_new_quartet_detected(toltec_db_with_new_quartet):
    """Test that query_toltec_db_quartet_status detects newer quartet."""
    with toltec_db_with_new_quartet.get_session() as session:
        status = query_toltec_db_quartet_status(
            "toltec", 123456, 0, 0, session=session
        )
    
    assert status["valid_count"] == 8, "Old quartet has 8 valid interfaces"
    assert status["new_quartet_detected"] is True, "Should detect newer quartet exists"


def test_sensor_creates_quartet_completion_request_on_timeout(
    toltec_db_with_complete_quartet
):
    """
    Test that sensor creates RunRequest for quartet_complete when timeout reached.
    
    This is the critical test verifying the bridge between per-interface (2D)
    and quartet-level (1D) triggering.
    """
    from dagster._core.definitions.resource_definition import ScopedResourcesBuilder
    
    instance = DagsterInstance.ephemeral()
    
    # Build sensor context with resource
    context = build_sensor_context(
        instance=instance,
        resources=ScopedResourcesBuilder().build({"toltec_db": toltec_db_with_complete_quartet}),
    )
    
    # Run sensor
    result = sync_with_toltec_db(context)
    
    # Convert generator to list
    if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
        requests = list(result)
    else:
        requests = [result] if result else []
    
    # Should have 13 interface requests + 1 quartet completion request
    assert len(requests) >= 1, "Should create at least one RunRequest"
    
    # Find quartet completion request
    quartet_requests = [
        r for r in requests
        if hasattr(r, 'tags') and r.tags.get("source") == "toltec_db_sync_quartet_completion"
    ]
    
    assert len(quartet_requests) == 1, "Should create exactly one quartet completion request"
    
    quartet_request = quartet_requests[0]
    assert quartet_request.partition_key == "toltec-123456-0-0"
    assert quartet_request.tags["completion_reason"] == "timeout"
    assert quartet_request.tags["valid_count"] == "13"


def test_sensor_no_quartet_completion_request_when_incomplete(
    toltec_db_with_incomplete_quartet
):
    """
    Test that sensor does NOT create quartet completion request when incomplete.
    
    Quartet has only 8/13 valid and time_since_last_valid < 30s, so should not
    trigger quartet_complete yet.
    """
    from dagster._core.definitions.resource_definition import ScopedResourcesBuilder
    
    instance = DagsterInstance.ephemeral()
    
    context = build_sensor_context(
        instance=instance,
        resources=ScopedResourcesBuilder().build({"toltec_db": toltec_db_with_incomplete_quartet}),
    )
    
    result = sync_with_toltec_db(context)
    
    # Convert generator to list
    if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
        requests = list(result)
    else:
        requests = [result] if result else []
    
    # Should have interface requests but NO quartet completion request
    quartet_requests = [
        r for r in requests
        if hasattr(r, 'tags') and r.tags.get("source") == "toltec_db_sync_quartet_completion"
    ]
    
    assert len(quartet_requests) == 0, "Should NOT create quartet completion request yet"


def test_sensor_quartet_completion_request_on_new_quartet(
    toltec_db_with_new_quartet
):
    """
    Test that sensor creates quartet completion request when newer quartet detected.
    
    Old quartet has only 8/13 valid but a newer quartet exists, so old quartet
    is definitively complete.
    """
    from dagster._core.definitions.resource_definition import ScopedResourcesBuilder
    
    instance = DagsterInstance.ephemeral()
    
    context = build_sensor_context(
        instance=instance,
        resources=ScopedResourcesBuilder().build({"toltec_db": toltec_db_with_new_quartet}),
    )
    
    result = sync_with_toltec_db(context)
    
    # Convert generator to list
    if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
        requests = list(result)
    else:
        requests = [result] if result else []
    
    # Should have interface requests + 1 quartet completion request for old quartet
    quartet_requests = [
        r for r in requests
        if hasattr(r, 'tags') 
        and r.tags.get("source") == "toltec_db_sync_quartet_completion"
        and r.partition_key == "toltec-123456-0-0"  # Old quartet
    ]
    
    assert len(quartet_requests) == 1, "Should create quartet completion request for old quartet"
    
    quartet_request = quartet_requests[0]
    assert quartet_request.tags["completion_reason"] == "new_quartet"
    assert quartet_request.tags["valid_count"] == "8"
