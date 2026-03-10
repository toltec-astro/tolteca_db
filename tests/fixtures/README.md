# Test Fixtures with Real Database Data

## Overview

The test suite now includes fixtures that create in-memory databases populated with **real sample data** from the production toltec database. This catches bugs that mocked tests miss, particularly:

- Type mismatches between partition key extraction and database queries
- SQL query building errors
- Data serialization issues
- Case sensitivity issues

## Bug This Would Have Caught

**Bug**: In `raw_obs_product` asset, the variable `interface` (string like `"toltec6"`) was used directly in database queries expecting `nw_id` as an integer, causing:

```
_duckdb.ConversionException: Could not convert string 'toltec6' to INT32
```

**Why mocked tests missed it**:
1. Mock data used pre-converted integers: `{"interface": 4}` 
2. No actual database query execution
3. No real partition key extraction (used simple strings)

**How real database fixtures catch it**:
1. Uses actual `MultiPartitionKey` format: `{"interface": "toltec6"}`
2. Extracts values as production code does (returns strings)
3. Executes real SQL queries against in-memory database
4. Type mismatches cause immediate failures

## Available Fixtures

### `sample_toltec_db_engine` (scope: session)

Creates in-memory SQLite database with real sample data from `toltecdb_last_30days.sqlite`:

- **Master** entries (toltec, tcs, ics)
- **RawObs** entries (3 recent observations, ~20 entries)
- **InterfaceFile** entries (~100 entries, both valid and invalid)

```python
@pytest.mark.integration
def test_with_real_data(sample_toltec_db_session):
    """Test using real database entries."""
    # Query real RawObs entries
    result = sample_toltec_db_session.execute(
        select(RawObs).limit(1)
    ).scalar_one()
    
    assert result.obsnum > 0
```

### `sample_tolteca_db_engine` (scope: session)

Creates in-memory DuckDB database for tolteca_db (data products):

- **DataProdType** entries (dp_raw_obs, dp_reduced, dp_cal)
- Schema for **DataProd**, **DataProdSource**, etc.

```python
def test_data_product_creation(sample_tolteca_db_session):
    """Test DataProd creation logic."""
    dp_type = sample_tolteca_db_session.execute(
        select(DataProdType).where(DataProdType.name == "dp_raw_obs")
    ).scalar_one()
    
    # Create DataProd entry
    data_prod = DataProd(
        data_prod_type_fk=dp_type.pk,
        meta=RawObsMeta(...)
    )
```

### `sample_toltec_db_session` (scope: function)

Session for `sample_toltec_db_engine` with automatic rollback.

### `sample_tolteca_db_session` (scope: function)

Session for `sample_tolteca_db_engine` with automatic rollback.

## Integration Test Examples

See `tests/test_dagster/test_integration_real_db.py`:

### 1. Partition Key Type Testing

```python
def test_partition_key_extraction_types(sample_toltec_db_session):
    """Verify partition key extraction produces correct types."""
    partition_key = MultiPartitionKey({
        "quartet": "toltec-12345-0-0",
        "interface": "toltec6"
    })
    
    interface = partition_key.keys_by_dimension["interface"]
    assert isinstance(interface, str)  # STRING from partition
    
    roach_index = get_interface_roach_index(interface)
    assert isinstance(roach_index, int)  # INTEGER for database
```

### 2. Database Query Building

```python
def test_raw_obs_product_query_building(
    sample_toltec_db_session, 
    sample_tolteca_db_session
):
    """Test query building with correct types."""
    # Get real raw_obs entry
    raw_obs = sample_toltec_db_session.execute(
        select(RawObs).limit(1)
    ).scalar_one()
    
    # Build query with correct types
    stmt = (
        select(DataProd)
        .where(DataProd.meta['nw_id'].as_integer() == roach_index)
    )
    
    # Execute query - will fail if types are wrong
    result = sample_tolteca_db_session.execute(stmt).first()
```

### 3. Bug Regression Test

```python
def test_interface_string_to_roach_index_conversion():
    """Test that would have caught the interface type bug."""
    interface = "toltec6"  # STRING from partition
    
    # WRONG: Using string directly
    # meta = RawObsMeta(nw_id=interface)  # Type error!
    
    # CORRECT: Convert to integer
    roach_index = get_interface_roach_index(interface)
    meta = RawObsMeta(nw_id=roach_index)  # Integer
```

## Setup Requirements

Tests require access to production database at:
```
../run/toltecdb_last_30days.sqlite
```

If not found, tests are automatically skipped with:
```python
pytest.skip(f"Production database not found: {prod_db}")
```

## Running Integration Tests

```bash
# Run all integration tests
uv run pytest -m integration -v

# Run specific integration test file
uv run pytest tests/test_dagster/test_integration_real_db.py -v

# Run with verbose output
uv run pytest tests/test_dagster/test_integration_real_db.py -vv
```

## Benefits

1. **Catches Type Mismatches**: Real queries expose type conversions that mocks hide
2. **Tests SQL Generation**: Verifies query building logic actually works
3. **Real Data Validation**: Uses actual data patterns from production
4. **Prevents Regressions**: Fixtures ensure bugs stay fixed
5. **Fast Execution**: In-memory databases are still fast (~seconds for full suite)

## Adding New Integration Tests

1. Use `sample_toltec_db_session` for tests querying toltec_db (RawObs, InterfaceFile)
2. Use `sample_tolteca_db_session` for tests creating DataProd entries
3. Mark with `@pytest.mark.integration`
4. Verify correct types are extracted and used
5. Execute actual queries to expose issues

Example:
```python
@pytest.mark.integration
def test_new_feature(sample_toltec_db_session):
    """Test new feature with real data."""
    # Query real data
    result = sample_toltec_db_session.execute(...)
    
    # Verify behavior
    assert result is not None
```

## Maintenance

Fixtures are automatically populated from production database at test runtime. No manual data maintenance required. If production database schema changes, fixtures automatically reflect updates.
