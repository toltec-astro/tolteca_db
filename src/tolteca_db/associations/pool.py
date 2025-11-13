"""
Association Pool - In-memory batch container for observations.

Provides fast filtering and candidate extraction for association generation.
Backend-agnostic: works with database queries or filesystem iterators.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from tolteca_db.constants import ToltecDataKind


__all__ = ["ObservationPool", "PoolRow"]


@dataclass
class PoolRow:
    """Single observation row from pool with extracted metadata."""

    pk: int
    obsnum: int | None
    subobsnum: int | None
    scannum: int | None
    master: int | None
    roachid: int | None
    data_kind: ToltecDataKind | None
    obs_goal: str | None
    interface: int | None
    raw: dict[str, Any]  # Full observation for detailed access


class ObservationPool:
    """
    In-memory batch of observations for fast association processing.

    Architecture:
    - Loads batch of observations (from DB or filesystem)
    - Extracts metadata into DataFrame for fast filtering
    - Provides candidate extraction and subset operations
    - Backend-agnostic design works with any data source
    """

    def __init__(self, observations: list[Any]):
        """
        Initialize pool from list of observations.

        Parameters
        ----------
        observations : list[DataProd] or list[dict]
            Raw observations from database or other source.
            Must have: pk, metadata attributes
        """
        self._observations = observations
        self._build_dataframe()

    def _build_dataframe(self) -> None:
        """Build DataFrame with extracted metadata for fast filtering."""
        rows = []
        for obs in self._observations:
            # Extract metadata - handle both DataProd objects and dicts
            # Note: DataProd uses 'meta' not 'metadata'
            if hasattr(obs, "meta"):
                metadata = obs.meta
                pk = obs.pk
            elif hasattr(obs, "metadata"):
                metadata = obs.metadata
                pk = obs.pk
            else:
                metadata = obs.get("metadata", {}) or obs.get("meta", {})
                pk = obs.get("pk")

            # Extract data_kind from metadata union
            data_kind = self._extract_data_kind(metadata)

            # Extract fields - handle both dict and dataclass metadata
            if isinstance(metadata, dict):
                # Dict-style access
                rows.append(
                    {
                        "pk": pk,
                        "obsnum": metadata.get("obsnum"),
                        "subobsnum": metadata.get("subobsnum"),
                        "scannum": metadata.get("scannum"),
                        "master": metadata.get("master"),
                        "roachid": metadata.get("roachid"),
                        "data_kind": data_kind,
                        "obs_goal": metadata.get("obs_goal"),
                        "interface": metadata.get("interface"),
                    }
                )
            else:
                # Dataclass-style access (for AdaptixJSON)
                rows.append(
                    {
                        "pk": pk,
                        "obsnum": getattr(metadata, "obsnum", None),
                        "subobsnum": getattr(metadata, "subobsnum", None),
                        "scannum": getattr(metadata, "scannum", None),
                        "master": getattr(metadata, "master", None),
                        "roachid": getattr(metadata, "roachid", None),
                        "data_kind": data_kind,
                        "obs_goal": getattr(metadata, "obs_goal", None),
                        "interface": getattr(metadata, "interface", None),
                    }
                )

        self.data = pd.DataFrame(rows)

        # Build lookup for fast access to full observation objects
        self._obs_by_pk = {obs.pk if hasattr(obs, "pk") else obs["pk"]: obs for obs in self._observations}

    def _extract_data_kind(self, metadata: Any) -> ToltecDataKind | None:
        """
        Extract ToltecDataKind from metadata union types.

        Handles: RawObsMeta, CalGroupMeta, DrivefitMeta, FocusGroupMeta, etc.
        """
        if metadata is None:
            return None

        # Try direct attribute access first
        if hasattr(metadata, "data_kind"):
            return metadata.data_kind

        # Try dict-like access
        if isinstance(metadata, dict):
            return metadata.get("data_kind")

        # Try to find data_kind in any nested attribute
        for attr_name in dir(metadata):
            if attr_name.startswith("_"):
                continue
            try:
                attr = getattr(metadata, attr_name)
                if isinstance(attr, ToltecDataKind):
                    return attr
            except Exception:
                continue

        return None

    def filter_by(self, **criteria) -> pd.DataFrame:
        """
        Filter pool by metadata criteria.

        Parameters
        ----------
        **criteria : dict
            Key-value pairs for filtering (e.g., obsnum=113533, master=12)

        Returns
        -------
        pd.DataFrame
            Subset of pool matching all criteria
        """
        mask = pd.Series([True] * len(self.data))

        for key, value in criteria.items():
            if value is None:
                # None means "must be null"
                mask &= self.data[key].isna()
            else:
                mask &= self.data[key] == value

        return self.data[mask]

    def extract_candidates(self, group_by: str | list[str]) -> pd.DataFrame:
        """
        Extract unique candidate values from pool.

        Parameters
        ----------
        group_by : str or list[str]
            Column(s) to extract unique combinations from

        Returns
        -------
        pd.DataFrame
            Unique candidate values with counts

        Examples
        --------
        >>> pool.extract_candidates('obsnum')
        >>> pool.extract_candidates(['obsnum', 'master'])
        """
        if isinstance(group_by, str):
            group_by = [group_by]

        # Get unique combinations with counts
        candidates = (
            self.data.groupby(group_by, dropna=False).size().reset_index(name="count")
        )

        return candidates

    def get_observation(self, pk: int) -> Any:
        """
        Get full observation object by primary key.

        Parameters
        ----------
        pk : int
            Primary key of observation

        Returns
        -------
        DataProd or dict
            Full observation object
        """
        return self._obs_by_pk.get(pk)

    def get_observations(self, pks: list[int]) -> list[Any]:
        """
        Get multiple observation objects by primary keys.

        Parameters
        ----------
        pks : list[int]
            List of primary keys

        Returns
        -------
        list[DataProd or dict]
            List of full observation objects
        """
        return [self._obs_by_pk.get(pk) for pk in pks if pk in self._obs_by_pk]

    def __len__(self) -> int:
        """Return number of observations in pool."""
        return len(self.data)

    def __repr__(self) -> str:
        """Return string representation of pool."""
        return f"ObservationPool(n_observations={len(self)}, columns={list(self.data.columns)})"
