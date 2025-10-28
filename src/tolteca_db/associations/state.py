"""
Association State Tracking - Incremental processing support.

Tracks which observations are already grouped and maintains group index
for fast lookups. Supports both database and filesystem backends.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from tolteca_db.models.orm import DataProd, DataProdAssoc


__all__ = ["AssociationState", "GroupInfo", "StateBackend"]


@dataclass
class GroupInfo:
    """Information about an existing association group."""

    group_pk: int
    group_type: str  # 'cal_group', 'drivefit', 'focus'
    candidate_key: str  # Unique key identifying this group
    n_members: int
    metadata: dict[str, Any]  # Group metadata for reference


class StateBackend:
    """Base class for state persistence backends."""

    def load_grouped_observations(self) -> set[int]:
        """Load set of observation PKs that are already grouped."""
        raise NotImplementedError

    def load_group_index(self) -> dict[str, GroupInfo]:
        """Load index of existing groups: {candidate_key: GroupInfo}."""
        raise NotImplementedError

    def save_grouped_observations(self, grouped_obs: set[int]) -> None:
        """Save updated set of grouped observations."""
        raise NotImplementedError

    def save_group_index(self, group_index: dict[str, GroupInfo]) -> None:
        """Save updated group index."""
        raise NotImplementedError


class DatabaseBackend(StateBackend):
    """State backend using SQLAlchemy database."""

    def __init__(self, session: Session):
        self.session = session

    def load_grouped_observations(self) -> set[int]:
        """Query database for all observations that have associations."""
        grouped = (
            self.session.query(DataProdAssoc.dst_data_prod_fk).distinct().scalars()
        )
        return set(grouped)

    def load_group_index(self) -> dict[str, GroupInfo]:
        """Build index from existing group DataProds."""
        index = {}

        # Query all group type DataProds (types > 1 are groups)
        groups = self.session.query(DataProd).filter(DataProd.type_fk > 1).all()

        for group in groups:
            # Extract candidate key from metadata
            candidate_key = self._extract_candidate_key(group)
            if candidate_key:
                # Get member count
                n_members = (
                    self.session.query(DataProdAssoc)
                    .filter(DataProdAssoc.src_data_prod_fk == group.pk)
                    .count()
                )

                # Get group type label
                group_type = self._get_group_type_label(group.type_fk)

                # Store group info
                info = GroupInfo(
                    group_pk=group.pk,
                    group_type=group_type,
                    candidate_key=candidate_key,
                    n_members=n_members,
                    metadata=self._serialize_metadata(group.metadata),
                )
                index[candidate_key] = info

        return index

    def _extract_candidate_key(self, group: DataProd) -> str | None:
        """
        Extract candidate key from group metadata.

        Different group types use different keys:
        - CalGroup: f"cal_{obsnum}_{master}"
        - DriveFit: f"drivefit_{obsnum}_{master}"
        - FocusGroup: f"focus_{obsnum}_{master}"
        """
        if group.metadata is None:
            return None

        # Extract fields from metadata
        obsnum = getattr(group.metadata, "obsnum", None)
        master = getattr(group.metadata, "master", None)

        if obsnum is None or master is None:
            return None

        # Determine group type from type_fk
        group_type = self._get_group_type_label(group.type_fk)

        return f"{group_type}_{obsnum}_{master}"

    def _get_group_type_label(self, type_fk: int) -> str:
        """Map type_fk to group type label."""
        # This mapping should match your DataProdType enum
        # TODO: Query from database or use enum
        type_map = {
            1: "raw_obs",
            2: "cal_group",
            3: "drivefit",
            4: "focus",
        }
        return type_map.get(type_fk, f"type_{type_fk}")

    def _serialize_metadata(self, metadata: Any) -> dict[str, Any]:
        """Convert metadata to dict for JSON serialization."""
        if metadata is None:
            return {}
        if isinstance(metadata, dict):
            return metadata
        # For dataclass metadata
        try:
            return asdict(metadata)
        except Exception:
            return {"_raw": str(metadata)}

    def save_grouped_observations(self, grouped_obs: set[int]) -> None:
        """No-op for database backend - state is live in DB."""
        pass

    def save_group_index(self, group_index: dict[str, GroupInfo]) -> None:
        """No-op for database backend - state is live in DB."""
        pass


class FilesystemBackend(StateBackend):
    """State backend using JSON index files on filesystem."""

    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.grouped_obs_file = self.state_dir / "grouped_observations.json"
        self.group_index_file = self.state_dir / "group_index.json"

    def load_grouped_observations(self) -> set[int]:
        """Load grouped observations from JSON file."""
        if not self.grouped_obs_file.exists():
            return set()

        with open(self.grouped_obs_file) as f:
            data = json.load(f)
            return set(data.get("grouped_obs", []))

    def load_group_index(self) -> dict[str, GroupInfo]:
        """Load group index from JSON file."""
        if not self.group_index_file.exists():
            return {}

        with open(self.group_index_file) as f:
            data = json.load(f)
            index = {}
            for key, info_dict in data.items():
                index[key] = GroupInfo(**info_dict)
            return index

    def save_grouped_observations(self, grouped_obs: set[int]) -> None:
        """Save grouped observations to JSON file."""
        data = {"grouped_obs": sorted(list(grouped_obs))}
        with open(self.grouped_obs_file, "w") as f:
            json.dump(data, f, indent=2)

    def save_group_index(self, group_index: dict[str, GroupInfo]) -> None:
        """Save group index to JSON file."""
        data = {key: asdict(info) for key, info in group_index.items()}
        with open(self.group_index_file, "w") as f:
            json.dump(data, f, indent=2)


class AssociationState:
    """
    Track association state for incremental processing.

    Maintains:
    - Set of observation PKs that are already grouped
    - Index of existing groups by candidate key
    - Dirty flags for persistence

    Supports both database and filesystem backends.
    """

    def __init__(self, backend: StateBackend):
        """
        Initialize state with given backend.

        Parameters
        ----------
        backend : StateBackend
            Database or filesystem backend for persistence
        """
        self.backend = backend
        self._grouped_obs: set[int] = set()
        self._group_index: dict[str, GroupInfo] = {}
        self._dirty_grouped = False
        self._dirty_index = False

        # Load initial state
        self._load()

    def _load(self) -> None:
        """Load state from backend."""
        self._grouped_obs = self.backend.load_grouped_observations()
        self._group_index = self.backend.load_group_index()
        self._dirty_grouped = False
        self._dirty_index = False

    def is_grouped(self, obs_pk: int) -> bool:
        """
        Check if observation is already in a group.

        Parameters
        ----------
        obs_pk : int
            Observation primary key

        Returns
        -------
        bool
            True if observation is already grouped
        """
        return obs_pk in self._grouped_obs

    def get_ungrouped(self, obs_pks: list[int]) -> list[int]:
        """
        Filter to observations not yet grouped.

        Parameters
        ----------
        obs_pks : list[int]
            List of observation PKs to check

        Returns
        -------
        list[int]
            Subset of PKs that are not yet grouped
        """
        return [pk for pk in obs_pks if pk not in self._grouped_obs]

    def get_existing_group(self, candidate_key: str) -> GroupInfo | None:
        """
        Look up existing group by candidate key.

        Parameters
        ----------
        candidate_key : str
            Unique key identifying the group

        Returns
        -------
        GroupInfo or None
            Group information if exists, None otherwise
        """
        return self._group_index.get(candidate_key)

    def mark_grouped(self, obs_pk: int) -> None:
        """
        Mark observation as grouped.

        Parameters
        ----------
        obs_pk : int
            Observation primary key
        """
        if obs_pk not in self._grouped_obs:
            self._grouped_obs.add(obs_pk)
            self._dirty_grouped = True

    def register_group(self, group_info: GroupInfo) -> None:
        """
        Register new or updated group in index.

        Parameters
        ----------
        group_info : GroupInfo
            Group information to register
        """
        self._group_index[group_info.candidate_key] = group_info
        self._dirty_index = True

    def update_group_member_count(self, candidate_key: str, n_members: int) -> None:
        """
        Update member count for existing group.

        Parameters
        ----------
        candidate_key : str
            Unique key identifying the group
        n_members : int
            New member count
        """
        if candidate_key in self._group_index:
            self._group_index[candidate_key].n_members = n_members
            self._dirty_index = True

    def flush(self) -> None:
        """Persist dirty state to backend."""
        if self._dirty_grouped:
            self.backend.save_grouped_observations(self._grouped_obs)
            self._dirty_grouped = False

        if self._dirty_index:
            self.backend.save_group_index(self._group_index)
            self._dirty_index = False

    def reload(self) -> None:
        """Reload state from backend (discard in-memory changes)."""
        self._load()

    def stats(self) -> dict[str, Any]:
        """
        Get state statistics.

        Returns
        -------
        dict
            Statistics about current state
        """
        return {
            "n_grouped_observations": len(self._grouped_obs),
            "n_groups": len(self._group_index),
            "groups_by_type": self._count_groups_by_type(),
            "dirty_grouped": self._dirty_grouped,
            "dirty_index": self._dirty_index,
        }

    def _count_groups_by_type(self) -> dict[str, int]:
        """Count groups by type."""
        counts = defaultdict(int)
        for info in self._group_index.values():
            counts[info.group_type] += 1
        return dict(counts)

    def __repr__(self) -> str:
        """Return string representation."""
        stats = self.stats()
        return (
            f"AssociationState("
            f"n_grouped={stats['n_grouped_observations']}, "
            f"n_groups={stats['n_groups']}, "
            f"dirty={stats['dirty_grouped'] or stats['dirty_index']})"
        )
