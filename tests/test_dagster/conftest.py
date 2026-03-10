"""Test configuration for Dagster tests."""

from __future__ import annotations

import warnings

import pytest


@pytest.fixture(scope="session", autouse=True)
def suppress_dagster_config_warnings():
    """Suppress Dagster ConfigArgumentWarning for test execution."""
    try:
        from dagster_shared.utils.warnings import ConfigArgumentWarning

        warnings.filterwarnings("ignore", category=ConfigArgumentWarning)
    except ImportError:
        pass
