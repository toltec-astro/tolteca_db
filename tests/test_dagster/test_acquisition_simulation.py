"""Integration tests for acquisition simulation and quartet completion.

These tests verify the complete workflow:
1. Load data into test database
2. Simulate Invalid → Valid transitions
3. Verify quartet completion logic
4. Test timeout and disabled interface handling

Run with: pytest tests/test_dagster/test_acquisition_simulation.py -v
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from sqlalchemy import text


@pytest.mark.integration
class TestAcquisitionSimulation:
    """Test simulated data acquisition workflow."""

    def test_test_database_initialization(self, test_toltec_db_resource):
        """Verify test database schema is created correctly."""
        session = test_toltec_db_resource.get_session()

        # Verify tables exist
        tables = session.execute(
            text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        ).fetchall()

        table_names = [t[0] for t in tables]
        assert "master" in table_names
        assert "raw_obs" in table_names
        assert "interface_file" in table_names

        # Verify master entries
        masters = session.execute(
            text("SELECT label FROM master ORDER BY id")
        ).fetchall()
        assert len(masters) == 3
        assert masters[0][0] == "TCS"
        assert masters[1][0] == "TOLTEC"
        assert masters[2][0] == "ICS"

    def test_insert_test_raw_obs(self, test_toltec_db_resource):
        """Test inserting raw_obs entries for simulation."""
        session = test_toltec_db_resource.get_session()

        # Insert test raw_obs
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )
        session.commit()

        # Verify insertion
        result = session.execute(text("SELECT obsnum FROM raw_obs WHERE id=1")).scalar()
        assert result == 99999

    def test_insert_invalid_interfaces(self, test_toltec_db_resource):
        """Test inserting interface_file entries in Invalid state."""
        session = test_toltec_db_resource.get_session()

        # Insert raw_obs first
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        # Insert 10 interfaces (excluding disabled [5, 6, 10])
        expected_interfaces = [0, 1, 2, 3, 4, 7, 8, 9, 11, 12]
        for nw in expected_interfaces:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 0, :filename, NULL)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc"},
            )

        session.commit()

        # Verify all are invalid
        invalid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=0")
        ).scalar()
        assert invalid_count == 10

        # Verify none are valid yet
        valid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=1")
        ).scalar()
        assert valid_count == 0

    def test_simulate_validation_transition(self, test_toltec_db_resource):
        """Test simulating Invalid → Valid transition."""
        session = test_toltec_db_resource.get_session()

        # Setup: Insert raw_obs and invalid interfaces
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        for nw in [0, 1, 2]:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 0, :filename, NULL)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc"},
            )

        session.commit()

        # Simulate: Mark one interface as valid
        now = datetime.now(timezone.utc).isoformat()
        session.execute(
            text("""
            UPDATE interface_file
            SET valid=1, ut_acquired=:ut
            WHERE nw=0
        """),
            {"ut": now},
        )
        session.commit()

        # Verify: One valid, two invalid
        valid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=1")
        ).scalar()
        assert valid_count == 1

        invalid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=0")
        ).scalar()
        assert invalid_count == 2

        # Verify timestamp was set
        ut_acquired = session.execute(
            text("SELECT ut_acquired FROM interface_file WHERE nw=0")
        ).scalar()
        assert ut_acquired is not None
        assert ut_acquired == now

    def test_respect_disabled_interfaces(self, test_toltec_db_resource):
        """Test that disabled interfaces are not marked valid."""
        session = test_toltec_db_resource.get_session()

        # Setup: Insert raw_obs and ALL interfaces (including disabled)
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        disabled_interfaces = {5, 6, 10}
        for nw in range(13):
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 0, :filename, NULL)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc"},
            )

        session.commit()

        # Simulate: Mark NON-DISABLED interfaces valid
        now = datetime.now(timezone.utc).isoformat()
        for nw in range(13):
            if nw not in disabled_interfaces:
                session.execute(
                    text("""
                    UPDATE interface_file
                    SET valid=1, ut_acquired=:ut
                    WHERE nw=:nw
                """),
                    {"nw": nw, "ut": now},
                )

        session.commit()

        # Verify: 10 valid (13 - 3 disabled), 3 invalid (disabled)
        valid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=1")
        ).scalar()
        assert valid_count == 10

        invalid_count = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=0")
        ).scalar()
        assert invalid_count == 3

        # Verify disabled interfaces are still invalid
        for nw in disabled_interfaces:
            valid = session.execute(
                text("SELECT valid FROM interface_file WHERE nw=:nw"), {"nw": nw}
            ).scalar()
            assert valid == 0, f"Disabled interface {nw} should remain invalid"

    def test_gradual_validation_simulation(self, test_toltec_db_resource):
        """Test gradual Invalid → Valid transitions (batch updates)."""
        session = test_toltec_db_resource.get_session()

        # Setup: Insert raw_obs and 10 interfaces
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        expected_interfaces = [0, 1, 2, 3, 4, 7, 8, 9, 11, 12]
        for nw in expected_interfaces:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 0, :filename, NULL)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc"},
            )

        session.commit()

        # Simulate: Update 2 interfaces per cycle (5 cycles total)
        interfaces_per_update = 2
        for cycle in range(5):
            # Get next invalid interfaces
            invalid_interfaces = session.execute(
                text("""
                SELECT nw FROM interface_file 
                WHERE valid=0 
                LIMIT :limit
            """),
                {"limit": interfaces_per_update},
            ).fetchall()

            if not invalid_interfaces:
                break

            # Mark as valid
            now = datetime.now(timezone.utc).isoformat()
            for (nw,) in invalid_interfaces:
                session.execute(
                    text("""
                    UPDATE interface_file
                    SET valid=1, ut_acquired=:ut
                    WHERE nw=:nw
                """),
                    {"nw": nw, "ut": now},
                )

            session.commit()

            # Verify count increases
            valid_count = session.execute(
                text("SELECT COUNT(*) FROM interface_file WHERE valid=1")
            ).scalar()
            expected_valid = min((cycle + 1) * interfaces_per_update, 10)
            assert valid_count == expected_valid, (
                f"Cycle {cycle + 1}: expected {expected_valid} valid"
            )

        # Final verification: All 10 should be valid
        final_valid = session.execute(
            text("SELECT COUNT(*) FROM interface_file WHERE valid=1")
        ).scalar()
        assert final_valid == 10

    def test_query_time_since_last_valid(self, test_toltec_db_resource):
        """Test calculating time since last Valid=1 transition."""
        session = test_toltec_db_resource.get_session()

        # Setup: Insert raw_obs and interfaces with timestamps
        base_time = datetime.now(timezone.utc)
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": base_time.isoformat()},
        )

        # Insert interfaces with staggered validation times
        import time

        for nw in [0, 1, 2]:
            ut = datetime.now(timezone.utc).isoformat()
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 1, :filename, :ut)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc", "ut": ut},
            )
            time.sleep(0.1)  # Small delay between insertions

        session.commit()

        # Query most recent validation timestamp
        latest_ut = session.execute(
            text("""
            SELECT MAX(ut_acquired) 
            FROM interface_file 
            WHERE valid=1
        """)
        ).scalar()

        assert latest_ut is not None

        # Verify it's recent (within last second)
        latest_time = datetime.fromisoformat(latest_ut)
        time_diff = (datetime.now(timezone.utc) - latest_time).total_seconds()
        assert time_diff < 1.0, (
            f"Latest validation should be recent, got {time_diff}s ago"
        )


@pytest.mark.integration
class TestQuartetCompletionLogic:
    """Test quartet completion detection logic."""

    def test_completion_with_all_expected_interfaces(self, test_toltec_db_resource):
        """Test completion when all expected interfaces are valid."""
        session = test_toltec_db_resource.get_session()

        # Setup: All 10 expected interfaces valid
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        expected_interfaces = [0, 1, 2, 3, 4, 7, 8, 9, 11, 12]
        now = datetime.now(timezone.utc).isoformat()
        for nw in expected_interfaces:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 1, :filename, :ut)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc", "ut": now},
            )

        session.commit()

        # Verify all expected are valid
        valid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM interface_file 
            WHERE raw_obs_id=1 AND valid=1
        """)
        ).scalar()
        assert valid_count == 10

        # Check if ready for completion
        disabled_set = {5, 6, 10}
        expected_count = 13 - len(disabled_set)

        # This would trigger quartet completion
        assert valid_count >= expected_count

    def test_completion_criteria_with_timeout(self, test_toltec_db_resource):
        """Test timeout-based completion with missing interfaces."""
        session = test_toltec_db_resource.get_session()

        # Setup: Only 6 interfaces valid (not all 10 expected)
        session.execute(
            text("""
            INSERT INTO raw_obs 
            (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 99999, 0, 0, :ut, '{}')
        """),
            {"ut": datetime.now(timezone.utc).isoformat()},
        )

        # Mark only 6 as valid, with oldest timestamp 20s ago
        from datetime import timedelta

        old_time = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()

        for nw in [0, 1, 2, 3, 4, 7]:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 1, :filename, :ut)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc", "ut": old_time},
            )

        # Add remaining as invalid
        for nw in [8, 9, 11, 12]:
            session.execute(
                text("""
                INSERT INTO interface_file
                (raw_obs_id, nw, valid, filename, ut_acquired)
                VALUES (1, :nw, 0, :filename, NULL)
            """),
                {"nw": nw, "filename": f"toltec{nw}_99999_0_0.nc"},
            )

        session.commit()

        # Check timeout condition (15s threshold in test mode)
        latest_ut = session.execute(
            text("""
            SELECT MAX(ut_acquired) 
            FROM interface_file 
            WHERE valid=1 AND raw_obs_id=1
        """)
        ).scalar()

        latest_time = datetime.fromisoformat(latest_ut)
        time_since_last = (datetime.now(timezone.utc) - latest_time).total_seconds()

        # Should trigger timeout completion (>15s since last valid)
        timeout_threshold = 15.0
        assert time_since_last > timeout_threshold, (
            f"Time since last valid ({time_since_last}s) should exceed threshold ({timeout_threshold}s)"
        )

        # Valid count check
        valid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM interface_file 
            WHERE raw_obs_id=1 AND valid=1
        """)
        ).scalar()
        assert valid_count == 6, "Only 6 interfaces should be valid"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
