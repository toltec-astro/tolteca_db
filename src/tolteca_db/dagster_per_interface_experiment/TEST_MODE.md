# Test Mode for Quartet Completion Testing

## Overview

This test mode allows you to simulate ongoing data acquisition and test the quartet completion behavior with declarative automation (`eager().without(~any_deps_missing())`).

**Key Features:**
- File-based SQLite test database (accessible by all Dagster components)
- Loads real data from production database
- Simulates Invalid → Valid transitions with configurable timing
- Tests timeout-based completion logic
- Tests disabled interface handling
- Tests eager automation condition behavior

## Quick Start

### 1. Enable Test Mode

Edit `definitions.py` to use test definitions:

```python
from tolteca_db.dagster.test_resources import create_test_definitions
import os

# Switch to test mode via environment variable
if os.getenv("DAGSTER_TEST_MODE") == "1":
    defs = create_test_definitions(
        test_db_path="./.dagster/test_toltec.sqlite",
        real_db_path="../run/toltecdb_last_30days.sqlite"
    )
else:
    # Normal production definitions
    defs = Definitions(...)
```

### 2. Run Dagster in Test Mode

```bash
# Enable test mode
export DAGSTER_TEST_MODE=1

# Start Dagster
cd tolteca_db
dagster dev
```

### 3. Execute Test Workflow

In Dagster UI (http://localhost:3000):

1. **Materialize `test_data_loader`**
   - Copies recent observations from real database
   - Marks all interfaces as Invalid (valid=0)
   - Creates starting state for simulation

2. **Materialize `acquisition_simulator`**
   - Gradually marks interfaces as Valid (valid=1)
   - Simulates integration time (5s between updates)
   - Updates 2 interfaces per cycle
   - Respects disabled interfaces [5, 6, 10]

3. **Monitor `quartet_complete`**
   - Automation sensor evaluates `eager().without(~any_deps_missing())`
   - Triggers when upstream `raw_obs_product` assets materialize
   - Tests timeout logic (15s in test mode vs 30s in prod)
   - Verifies disabled interface handling

## Test Scenarios

### Scenario 1: All Interfaces Complete

**Setup:**
- 13 interfaces, 3 disabled [5, 6, 10]
- Expected: 10 valid interfaces

**Expected Behavior:**
1. `acquisition_simulator` marks 2 interfaces valid every 5s
2. After 5 cycles (25s), all 10 expected interfaces valid
3. `quartet_complete` marks observation complete
4. Completion reason: "new quartet detected" or "expected interfaces validated"

### Scenario 2: Timeout with Missing Interface

**Setup:**
- Manually stop `acquisition_simulator` after 3 cycles
- Only 6 interfaces valid out of 10 expected

**Expected Behavior:**
1. No new Valid=1 transitions for 15s (validation_timeout_seconds)
2. `quartet_complete` marks observation complete via timeout
3. Completion reason: "timeout - no new validations in 15.0s"
4. Logs show: "6/10 expected interfaces validated"

### Scenario 3: Disabled Interface Never Validates

**Setup:**
- Disabled interfaces: [5, 6, 10]
- `acquisition_simulator` never marks these as valid

**Expected Behavior:**
1. Disabled interfaces remain Invalid throughout
2. Completion based on remaining 10 interfaces only
3. No timeout due to waiting for disabled interfaces
4. Logs show: "🚫 Disabled (configured): [5, 6, 10]"

### Scenario 4: Race Condition Prevention

**Setup:**
- `sync_with_toltec_db` sensor detects new Invalid entries
- Materializes `raw_obs_product` immediately

**Expected Behavior:**
1. `quartet_automation_sensor` evaluates `eager()` condition
2. Does NOT wait for all 13 interfaces (some disabled)
3. `.without(~any_deps_missing())` allows execution with missing deps
4. `quartet_complete` checks timeout logic, raises `DagsterExecutionInterruptedError`
5. Dagster retries automatically
6. Eventually completes when timeout reached or interfaces valid

## Configuration

### Test Resources

```python
# In test_resources.py

TestToltecDBResource:
  database_url: "sqlite:///./.dagster/test_toltec.sqlite"
  read_only: False  # Allow simulation writes

AcquisitionSimulatorConfig:
  source_db_path: "../run/toltecdb_last_30days.sqlite"
  interfaces_per_update: 2  # How many per cycle
  update_interval_seconds: 5.0  # Integration time simulation
  max_updates: 10  # Safety limit
  disabled_interfaces: [5, 6, 10]  # Match ValidationConfig

ValidationConfig (Test Mode):
  max_interface_count: 13
  disabled_interfaces: [5, 6, 10]
  validation_timeout_seconds: 15.0  # Faster for testing (vs 30s prod)
  sensor_poll_interval_seconds: 3  # More frequent (vs 5s prod)
  retry_on_incomplete: True
```

### Customizing Test Timing

Edit timing in `create_test_definitions()`:

```python
# Faster simulation (3s between updates)
AcquisitionSimulatorConfig(
    update_interval_seconds=3.0,
    interfaces_per_update=3,  # More aggressive
)

# Shorter timeout (10s)
ValidationConfig(
    validation_timeout_seconds=10.0,
)
```

## Verification Checklist

Use this checklist to verify the complete solution:

- [ ] **Test Data Loaded**
  - `test_data_loader` materializes successfully
  - Check logs: "Loaded N observations into test database"
  - Verify database: `sqlite3 .dagster/test_toltec.sqlite "SELECT COUNT(*) FROM interface_file WHERE valid=0"`

- [ ] **Acquisition Simulation Works**
  - `acquisition_simulator` materializes successfully
  - Check logs: "Marked interface N as VALID"
  - Gradual Invalid → Valid transitions (not all at once)
  - Disabled interfaces [5, 6, 10] never marked valid

- [ ] **Automation Sensor Running**
  - Check Dagster UI → Automation tab
  - `quartet_automation_sensor` status: Running (green)
  - Sensor evaluates every ~30s
  - Check logs: "Checking 1 assets/checks... for quartet_automation_sensor"

- [ ] **Eager Automation Triggers**
  - `raw_obs_product` materialization triggers evaluation
  - Check logs: "Tick produced N runs and M asset evaluations"
  - Does NOT wait for all 13 interfaces before triggering

- [ ] **Timeout Logic Works**
  - Stop `acquisition_simulator` mid-cycle
  - Wait 15s (validation_timeout_seconds)
  - `quartet_complete` marks complete via timeout
  - Check logs: "timeout - no new validations in 15.0s"

- [ ] **Disabled Interfaces Handled**
  - Interfaces [5, 6, 10] remain Invalid
  - Completion doesn't wait for them
  - Logs show: "🚫 Disabled (configured): [5, 6, 10]"
  - Expected count excludes disabled: "N/10 expected interfaces"

- [ ] **Race Condition Fixed**
  - No errors about "interface_file not found"
  - No premature quartet completion
  - `quartet_complete` only executes when upstream deps ready

## Debugging

### View Test Database

```bash
# Check interface_file status
sqlite3 .dagster/test_toltec.sqlite "
  SELECT nw, valid, ut_acquired 
  FROM interface_file 
  ORDER BY nw
"

# Check raw_obs entries
sqlite3 .dagster/test_toltec.sqlite "
  SELECT obsnum, subobsnum, scannum 
  FROM raw_obs
"

# Count valid vs invalid
sqlite3 .dagster/test_toltec.sqlite "
  SELECT 
    SUM(CASE WHEN valid=1 THEN 1 ELSE 0 END) as valid_count,
    SUM(CASE WHEN valid=0 THEN 1 ELSE 0 END) as invalid_count
  FROM interface_file
"
```

### Check Sensor Status

```bash
# View sensor evaluations
grep "quartet_automation_sensor" .dagster/logs/*.log | tail -20

# View automation condition evaluations
grep "Checking.*assets.*quartet_automation_sensor" .dagster/logs/*.log
```

### Monitor Completion Logic

```bash
# Watch quartet_complete executions
grep "quartet_complete" .dagster/logs/*.log | grep -E "(complete|timeout|interfaces)"

# View timeout calculations
grep "Time since last Valid=1" .dagster/logs/*.log
```

## Cleanup

```bash
# Remove test databases
rm .dagster/test_toltec.sqlite
rm .dagster/test_toltec_tolteca.duckdb

# Remove test data
rm -rf .dagster/test_data

# Exit test mode
unset DAGSTER_TEST_MODE
```

## Integration with pytest

You can also use the test database in pytest:

```python
import pytest
from tolteca_db.dagster.test_resources import TestToltecDBResource
from sqlalchemy import text

@pytest.fixture
def test_toltec_db():
    """Create test database for pytest."""
    db = TestToltecDBResource(
        database_url="sqlite:///.dagster/test_toltec.sqlite"
    )
    # Run setup
    class FakeContext:
        def log(self):
            return self
        def info(self, msg):
            print(msg)
    db.setup_for_execution(FakeContext())
    return db

def test_acquisition_simulation(test_toltec_db):
    """Test interface validation transitions."""
    session = test_toltec_db.get_session()
    
    # Get invalid interfaces
    result = session.execute(text(
        "SELECT COUNT(*) FROM interface_file WHERE valid=0"
    )).scalar()
    
    assert result > 0, "Should have invalid interfaces"
    
    # Simulate marking one valid
    session.execute(text("""
        UPDATE interface_file 
        SET valid=1 
        WHERE nw NOT IN (5, 6, 10)
        LIMIT 1
    """))
    session.commit()
    
    # Verify count decreased
    result_after = session.execute(text(
        "SELECT COUNT(*) FROM interface_file WHERE valid=0"
    )).scalar()
    
    assert result_after == result - 1, "One interface should be valid now"
```

## Notes

- **File-based database:** Required for Dagster resources to access across processes
- **In-memory won't work:** Dagster assets/sensors run in separate processes
- **Test timing:** Adjusted for faster feedback (15s timeout vs 30s prod)
- **Real data:** Copies from production database for realistic testing
- **Cleanup:** Test databases persist - delete manually when done
- **Disabled interfaces:** Must match between ValidationConfig and AcquisitionSimulatorConfig

## Future Enhancements

- [ ] Add pytest integration tests using test database
- [ ] Implement dynamic interface list from API
- [ ] Add test for sensor polling behavior
- [ ] Add test for concurrent quartet completion
- [ ] Add performance metrics collection
- [ ] Add test for error recovery scenarios
