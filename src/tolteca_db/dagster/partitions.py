"""Partition definitions for TolTEC data processing.

Simple 1D dynamic partitioning by quartet identifier.
Time-based partitioning for association generation.
"""

from __future__ import annotations

from dagster import DailyPartitionsDefinition, DynamicPartitionsDefinition

__all__ = [
    "quartet_partitions",
    "daily_partitions",
]

# 1D Dynamic Partitions: One partition per quartet
# Format: "ics-{obsnum}-{subobsnum}-{scannum}"
# Example: "ics-18846-0-0"
quartet_partitions = DynamicPartitionsDefinition(name="quartet")

# Daily Partitions: For association generation
# Associations span multiple observations, so use time-based partitions
# Start date: Beginning of TolTEC commissioning/science operations
daily_partitions = DailyPartitionsDefinition(start_date="2024-10-01")
