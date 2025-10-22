"""pytest configuration for tolteca_db tests."""

from __future__ import annotations

import pytest

from tolteca_db.utils import register_sqlite_adapters


@pytest.fixture(scope="session", autouse=True)
def _setup_sqlite_adapters():
    """Register SQLite datetime adapters before any tests run.
    
    This fixture automatically runs once per test session to configure
    SQLite's datetime handling, avoiding Python 3.12+ deprecation warnings.
    """
    register_sqlite_adapters()

