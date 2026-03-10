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

from sqlalchemy import func, select
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

    def load_grouped_observations(self) -> dict[int, set[int]]:
        """Load observations grouped by association type.
        
        Returns
        -------
        dict[int, set[int]]
            Mapping from data_prod_assoc_type_fk to set of observation PKs
        """
        raise NotImplementedError

    def load_group_index(self) -> dict[str, GroupInfo]:
        """Load index of existing groups: {candidate_key: GroupInfo}."""
        raise NotImplementedError

    def save_grouped_observations(self, grouped_obs: dict[int, set[int]]) -> None:
        """Save observations grouped by association type.
        
        Parameters
        ----------
        grouped_obs : dict[int, set[int]]
            Mapping from data_prod_assoc_type_fk to set of observation PKs
        """
        raise NotImplementedError

    def save_group_index(self, group_index: dict[str, GroupInfo]) -> None:
        """Save updated group index."""
        raise NotImplementedError


class DatabaseBackend(StateBackend):
    """State backend using SQLAlchemy database."""

    def __init__(self, session: Session):
        self.session = session

    def load_grouped_observations(self) -> dict[int, set[int]]:
        """Query database for observations grouped by association type.
        
        Returns
        -------
        dict[int, set[int]]
            Mapping from data_prod_assoc_type_fk to set of observation PKs
        """
        from sqlalchemy import select
        
        stmt = select(
            DataProdAssoc.data_prod_assoc_type_fk,
            DataProdAssoc.dst_data_prod_fk
        )
        results = self.session.execute(stmt).all()
        
        grouped_by_type = defaultdict(set)
        for assoc_type_fk, obs_pk in results:
            grouped_by_type[assoc_type_fk].add(obs_pk)
        
        return dict(grouped_by_type)

    def load_group_index(self) -> dict[str, GroupInfo]:
        """Build index from existing group DataProds.
        
        Uses a single aggregation query for member counts to avoid O(G) round-trips.
        """
        index = {}

        # Query all group type DataProds (types > 1 are groups)
        groups = self.session.scalars(
            select(DataProd).where(DataProd.data_prod_type_fk > 1)
        ).all()
        
        if not groups:
            return index
        
        # Single query to get all member counts - O(1) round-trip instead of O(G)
        group_pks = [g.pk for g in groups]
        count_stmt = (
            select(
                DataProdAssoc.src_data_prod_fk,
                func.count().label("n_members")
            )
            .where(DataProdAssoc.src_data_prod_fk.in_(group_pks))
            .group_by(DataProdAssoc.src_data_prod_fk)
        )
        member_counts = {
            row.src_data_prod_fk: row.n_members
            for row in self.session.execute(count_stmt).all()
        }

        for group in groups:
            # Extract candidate key from metadata
            candidate_key = self._extract_candidate_key(group)
            if candidate_key:
                # Get member count from pre-fetched map
                n_members = member_counts.get(group.pk, 0)

                # Get group type label
                group_type = self._get_group_type_label(group.data_prod_type_fk)

                # Store group info
                info = GroupInfo(
                    group_pk=group.pk,
                    group_type=group_type,
                    candidate_key=candidate_key,
                    n_members=n_members,
                    metadata=self._serialize_metadata(group.meta),
                )
                index[candidate_key] = info

        return index

    def _extract_candidate_key(self, group: DataProd) -> str | None:
        """
        Extract candidate key from group metadata.

        Uses obsnum and master (where applicable) to create a stable identifier.
        The key must NOT include n_items or name (which contains n_items) because
        these values change as groups grow, and we need to match existing groups.
        
        Keys:
        - CalGroup: f"dp_cal_group_{obsnum}_{master}"
        - DriveFit: f"dp_drivefit_{obsnum}_{master}"
        - FocusGroup: f"dp_focus_group_{obsnum}"
        - AstigGroup: f"dp_astig_group_{obsnum}"
        - OofGroup: f"dp_oof_group_{obsnum}"
        """
        if group.meta is None:
            return None

        # Extract obsnum - this is the first member's obsnum
        obsnum = getattr(group.meta, "obsnum", None)
        if obsnum is None:
            return None

        group_type = self._get_group_type_label(group.data_prod_type_fk)

        # Cal and Drivefit groups require master field
        if group_type in ("dp_cal_group", "dp_drivefit"):
            master = getattr(group.meta, "master", None)
            if master is None:
                return None
            return f"{group_type}_{obsnum}_{master}"

        # Focus, Astig, and Oof groups only use obsnum
        return f"{group_type}_{obsnum}"

    def _get_group_type_label(self, data_prod_type_fk: int) -> str:
        """Map data_prod_type_fk to group type label."""
        type_map = {
            1: "dp_raw_obs",
            3: "dp_cal_group",
            4: "dp_drivefit",
            5: "dp_focus_group",
            6: "dp_astig_group",
            7: "dp_oof_group",
        }
        return type_map.get(data_prod_type_fk, f"type_{data_prod_type_fk}")

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

    def load_grouped_observations(self) -> dict[int, set[int]]:
        """Load grouped observations from JSON file.
        
        Returns
        -------
        dict[int, set[int]]
            Mapping from assoc_type_fk to set of observation PKs
        """
        if not self.grouped_obs_file.exists():
            return {}

        with open(self.grouped_obs_file) as f:
            data = json.load(f)
            # Convert list values back to sets
            return {int(k): set(v) for k, v in data.get("grouped_by_type", {}).items()}

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

    def save_grouped_observations(self, grouped_obs: dict[int, set[int]]) -> None:
        """Save grouped observations to JSON file.
        
        Parameters
        ----------
        grouped_obs : dict[int, set[int]]
            Mapping from assoc_type_fk to set of observation PKs
        """
        # Convert sets to lists for JSON serialization
        data = {"grouped_by_type": {k: sorted(list(v)) for k, v in grouped_obs.items()}}
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
        self._grouped_obs_by_type: dict[int, set[int]] = {}
        self._group_index: dict[str, GroupInfo] = {}
        self._dirty_grouped = False
        self._dirty_index = False

        # Load initial state
        self._load()

    def _load(self) -> None:
        """Load state from backend."""
        self._grouped_obs_by_type = self.backend.load_grouped_observations()
        self._group_index = self.backend.load_group_index()
        self._dirty_grouped = False
        self._dirty_index = False

    def is_grouped(self, obs_pk: int, assoc_type_fk: int) -> bool:
        """
        Check if observation is already in a group of specific type.

        Parameters
        ----------
        obs_pk : int
            Observation primary key
        assoc_type_fk : int
            Association type foreign key

        Returns
        -------
        bool
            True if observation is already grouped for this association type
        """
        return obs_pk in self._grouped_obs_by_type.get(assoc_type_fk, set())

    def get_ungrouped(self, obs_pks: list[int], assoc_type_fk: int) -> list[int]:
        """
        Filter to observations not yet grouped for specific association type.

        Parameters
        ----------
        obs_pks : list[int]
            List of observation PKs to check
        assoc_type_fk : int
            Association type foreign key

        Returns
        -------
        list[int]
            Subset of PKs that are not yet grouped for this type
        """
        grouped_set = self._grouped_obs_by_type.get(assoc_type_fk, set())
        return [pk for pk in obs_pks if pk not in grouped_set]

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

    def mark_grouped(self, obs_pk: int, assoc_type_fk: int) -> None:
        """
        Mark observation as grouped for specific association type.

        Parameters
        ----------
        obs_pk : int
            Observation primary key
        assoc_type_fk : int
            Association type foreign key
        """
        if assoc_type_fk not in self._grouped_obs_by_type:
            self._grouped_obs_by_type[assoc_type_fk] = set()
        
        if obs_pk not in self._grouped_obs_by_type[assoc_type_fk]:
            self._grouped_obs_by_type[assoc_type_fk].add(obs_pk)
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
            self.backend.save_grouped_observations(self._grouped_obs_by_type)
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
