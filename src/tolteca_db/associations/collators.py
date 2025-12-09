"""Concrete collator implementations for different observation types.

This module provides collators for:
- CalGroup: Calibration sequences (VNA sweep followed by target sweeps)
- DriveFit: Drive characterization sequences (target sweeps with same obsnum)
- FocusGroup: Focus measurement sequences (observations with obs_goal='focus')
"""

from __future__ import annotations

from typing import ClassVar

from tolteca_db.constants import DataProdAssocType, DataProdType, ToltecDataKind
from tolteca_db.models.metadata import CalGroupMeta, DrivefitMeta, FocusGroupMeta, AstigGroupMeta

from .base import AssociationInfo, CollatorBase, Group, GroupFlag, Position

__all__ = [
    "CalGroupCollator",
    "DriveFitCollator",
    "FocusGroupCollator",
    "AstigmatismGroupCollator",
]


def _get_data_kind_union(data_prod) -> ToltecDataKind | None:
    """Extract union of data kinds from observation metadata."""
    if not data_prod.meta:
        return None
    
    data_kind_val = data_prod.meta.data_kind
    if data_kind_val is None:
        return None
    
    try:
        return ToltecDataKind(data_kind_val)
    except ValueError:
        return None


def _get_raw_obs_count(group: Group) -> int:
    """Count raw observations in a group."""
    return len([item for item in group.items if item.data_prod_type_fk == 1])  # dp_raw_obs


def _make_group_name(group: Group, suffix: str, count_func=None) -> str:
    """Generate a name for a data product group.
    
    Parameters
    ----------
    group : Group
        Group to name
    suffix : str
        Suffix to add to name (e.g., 'cal', 'drivefit', 'focus')
    count_func : callable, optional
        Function to count items in group for name
    
    Returns
    -------
    str
        Generated name like 'toltec-123456-g5-cal'
    """
    if not group.items:
        return f"empty-{suffix}"
    
    first = group.items[0]
    if not first.meta:
        return f"unknown-{suffix}"
    
    master = first.meta.master or "toltec"
    obsnum = first.meta.obsnum or 0
    
    if count_func:
        count = count_func(group)
    else:
        count = len(group.items)
    
    return f"{master}-{obsnum}-g{count}-{suffix}"


class CollateByPosition(CollatorBase):
    """Base for collators that group by sequential position.
    
    This collator identifies groups by analyzing the position of observations
    in a sequence (start, middle, end). Used for sequences that have explicit
    markers like VNA sweeps (start) and final target sweeps (end).
    """

    def _get_item_position(self, data_prod) -> Position | None:
        """Determine position of an observation in a sequence.
        
        Parameters
        ----------
        data_prod : DataProd
            Observation to analyze
            
        Returns
        -------
        Position | None
            Position in sequence, or None if not applicable
        """
        raise NotImplementedError

    def _filter_main_items(self, observations: list) -> list:
        """Filter observations to only those that can be main items.
        
        Parameters
        ----------
        observations : list
            All observations to consider
            
        Returns
        -------
        list
            Filtered observations that can be main group items
        """
        # Default: all raw observations
        return [obs for obs in observations if obs.data_prod_type_fk == 1]

    def make_groups(self, observations: list) -> list[Group]:
        """Identify groups by position in sequence.
        
        Parameters
        ----------
        observations : list
            Observations sorted by time
            
        Returns
        -------
        list[Group]
            Identified groups
        """
        main_items = self._filter_main_items(observations)
        if not main_items:
            return []
        
        # Build list of (position, observation) tuples
        pos_items = []
        for obs in main_items:
            pos = self._get_item_position(obs)
            if pos is not None:
                pos_items.append((pos, obs))
        
        if not pos_items:
            return []
        
        # Group by position
        groups = [Group()]
        for pos, obs in pos_items:
            if pos == Position.START:
                # Start new group
                groups.append(Group(flag=GroupFlag.EXPLICIT_START, items=[obs]))
            elif pos == Position.END:
                # End current group
                groups[-1].append(obs, add_flag=GroupFlag.EXPLICIT_END)
                groups.append(Group())
            else:
                # Add to current group if it's still open
                if groups[-1].flag & GroupFlag.EXPLICIT_END:
                    # Group already ended, skip
                    continue
                groups[-1].append(obs)
        
        # Filter to only explicit groups with items
        return [g for g in groups if g.items and (g.flag & GroupFlag.EXPLICIT)]


class CollateByMetadata(CollatorBase):
    """Base for collators that group by matching metadata.
    
    This collator groups observations that share common metadata values
    (e.g., same obsnum, same obs_goal). Used for sequences where all
    observations in the group have the same identifying characteristics.
    """

    # Subclasses define these
    collate_by_meta_keys: ClassVar[tuple[str, ...]]
    collate_by_meta_values_allowed: ClassVar[tuple[None | tuple, ...]]

    def _filter_items(self, observations: list) -> list:
        """Filter observations to only those eligible for grouping.
        
        Parameters
        ----------
        observations : list
            All observations to consider
            
        Returns
        -------
        list
            Filtered observations
        """
        # Default: return all
        return observations

    def _extract_key(self, data_prod) -> tuple | None:
        """Extract grouping key from observation metadata.
        
        Parameters
        ----------
        data_prod : DataProd
            Observation to extract key from
            
        Returns
        -------
        tuple | None
            Grouping key, or None if not applicable
        """
        if not data_prod.meta:
            return None
        
        key_values = []
        for i, key in enumerate(self.collate_by_meta_keys):
            value = getattr(data_prod.meta, key, None)
            if value is None:
                return None
            
            # Check if value is in allowed list
            allowed = self.collate_by_meta_values_allowed[i]
            if allowed is not None and value not in allowed:
                return None
            
            key_values.append(value)
        
        return tuple(key_values)

    def make_groups(self, observations: list) -> list[Group]:
        """Identify groups by matching metadata.
        
        Parameters
        ----------
        observations : list
            Observations to analyze
            
        Returns
        -------
        list[Group]
            Identified groups
        """
        filtered = self._filter_items(observations)
        if not filtered:
            return []
        
        # Group by key
        groups_by_key = {}
        for obs in filtered:
            key = self._extract_key(obs)
            if key is None:
                continue
            
            if key not in groups_by_key:
                groups_by_key[key] = Group()
            
            groups_by_key[key].append(obs)
        
        # Return groups with multiple items
        return [g for g in groups_by_key.values() if len(g.items) > 1]


class CollateByConsecutiveObsnum(CollatorBase):
    """Base for collators that group by consecutive observation numbers.
    
    This collator identifies sequences of observations with consecutive obsnums
    that share a common characteristic (e.g., obs_goal='focus'). Groups are
    split when there's a gap in the obsnum sequence.
    
    Used for sequences like focus measurements and astigmatism measurements
    where consecutive observations form a logical group.
    """

    # Subclasses define these
    obs_goal_filter: ClassVar[tuple[str, ...]]  # Allowed obs_goal values

    def _filter_items(self, observations: list) -> list:
        """Filter observations by obs_goal.
        
        Parameters
        ----------
        observations : list
            All observations to consider
            
        Returns
        -------
        list
            Filtered observations with matching obs_goal
        """
        filtered = []
        for obs in observations:
            if not obs.meta:
                continue
            
            obs_goal = getattr(obs.meta, 'obs_goal', None)
            if obs_goal in self.obs_goal_filter:
                filtered.append(obs)
        
        return filtered

    def make_groups(self, observations: list) -> list[Group]:
        """Identify groups by consecutive obsnums.
        
        Parameters
        ----------
        observations : list
            Observations sorted by time/obsnum
            
        Returns
        -------
        list[Group]
            Identified groups with consecutive obsnums
        """
        filtered = self._filter_items(observations)
        if not filtered:
            return []
        
        # Sort by (master, obsnum) to ensure proper ordering
        filtered.sort(key=lambda obs: (
            obs.meta.master if obs.meta else "",
            obs.meta.obsnum if obs.meta else 0
        ))
        
        # Group consecutive obsnums
        groups = []
        current_group = Group()
        prev_master = None
        prev_obsnum = None
        
        for obs in filtered:
            if not obs.meta:
                continue
            
            master = obs.meta.master
            obsnum = obs.meta.obsnum
            
            if obsnum is None:
                continue
            
            # Start new group if:
            # 1. First observation
            # 2. Master changed
            # 3. Obsnum not consecutive (gap > 1)
            if (prev_obsnum is None or 
                master != prev_master or 
                obsnum != prev_obsnum + 1):
                # Save current group if it has items
                if current_group.items:
                    groups.append(current_group)
                current_group = Group()
            
            current_group.append(obs)
            prev_master = master
            prev_obsnum = obsnum
        
        # Add final group
        if current_group.items:
            groups.append(current_group)
        
        # Return groups with 2+ items
        return [g for g in groups if len(g.items) >= 2]


class CalGroupCollator(CollateByPosition):
    """Collator for calibration sequences.
    
    Identifies groups of observations that form calibration sequences:
    - Starts with VNA sweep
    - Contains target sweeps
    - May include reduced observations
    
    Groups are formed when a VNA sweep is followed by target sweeps.
    """

    data_prod_type: ClassVar[str] = DataProdType.DP_CAL_GROUP.value
    data_prod_assoc_type: ClassVar[str] = DataProdAssocType.DPA_CAL_GROUP_RAW_OBS.value

    def _get_item_position(self, data_prod) -> Position | None:
        """Determine if observation starts or continues a cal sequence."""
        if data_prod.data_prod_type_fk != 1:  # Not raw obs
            return None
        
        data_kind = _get_data_kind_union(data_prod)
        if data_kind is None:
            return None
        
        # Check if this is a sweep observation
        if not (ToltecDataKind.RawSweep & data_kind):
            return None
        
        # VNA sweeps start calibration sequences
        if ToltecDataKind.VnaSweep & data_kind:
            return Position.START
        
        # Target sweeps continue sequences
        return Position.NOT_START

    def make_groups(self, observations: list) -> list[Group]:
        """Create calibration groups, filtering to those with 2+ observations."""
        groups = super().make_groups(observations)
        return [g for g in groups if _get_raw_obs_count(g) > 1]

    def _make_meta(self, group: Group) -> CalGroupMeta:
        """Create metadata for calibration group."""
        if not group.items or not group.items[0].meta:
            master = "toltec"
            obsnum = 0
        else:
            first = group.items[0].meta
            master = first.master or "toltec"
            obsnum = first.obsnum or 0
        
        n_items = _get_raw_obs_count(group)
        name = f"{master}-{obsnum}-g{n_items}-cal"
        
        return CalGroupMeta(
            name=name,
            data_prod_type=DataProdType.DP_CAL_GROUP,
            master=master,
            obsnum=obsnum,
            n_items=n_items,
            group_type="auto",
        )


class DriveFitCollator(CollateByMetadata):
    """Collator for drive characterization sequences.
    
    Identifies groups of target sweep observations with the same obsnum and master.
    These are used to characterize detector response curves.
    """

    data_prod_type: ClassVar[str] = DataProdType.DP_DRIVEFIT.value
    data_prod_assoc_type: ClassVar[str] = DataProdAssocType.DPA_DRIVEFIT_RAW_OBS.value
    collate_by_meta_keys: ClassVar[tuple[str, ...]] = ("obsnum", "master")
    collate_by_meta_values_allowed: ClassVar[tuple[None | tuple, ...]] = (None, None)

    def _filter_items(self, observations: list) -> list:
        """Filter to only target sweep observations."""
        filtered = []
        for obs in observations:
            data_kind = _get_data_kind_union(obs)
            if data_kind and (data_kind & ToltecDataKind.TargetSweep):
                filtered.append(obs)
        return filtered

    def make_groups(self, observations: list) -> list[Group]:
        """Create drivefit groups, filtering to those with 2+ observations."""
        groups = super().make_groups(observations)
        return [g for g in groups if _get_raw_obs_count(g) > 1]

    def _make_meta(self, group: Group) -> DrivefitMeta:
        """Create metadata for drivefit group."""
        if not group.items or not group.items[0].meta:
            master = "toltec"
            obsnum = 0
        else:
            first = group.items[0].meta
            master = first.master or "toltec"
            obsnum = first.obsnum or 0
        
        n_items = _get_raw_obs_count(group)
        name = f"{master}-{obsnum}-g{n_items}-drivefit"
        
        return DrivefitMeta(
            name=name,
            data_prod_type=DataProdType.DP_DRIVEFIT,
            master=master,
            obsnum=obsnum,
            n_items=n_items,
        )


class FocusGroupCollator(CollateByConsecutiveObsnum):
    """Collator for focus measurement sequences.
    
    Identifies groups of consecutive observations with obs_goal='focus'.
    These are used to measure and optimize telescope focus.
    
    Groups are formed from consecutive obsnums (e.g., 145647, 145648, 145649).
    A gap in obsnum sequence starts a new group.
    """

    data_prod_type: ClassVar[str] = DataProdType.DP_FOCUS_GROUP.value
    data_prod_assoc_type: ClassVar[str] = DataProdAssocType.DPA_FOCUS_GROUP_RAW_OBS.value
    obs_goal_filter: ClassVar[tuple[str, ...]] = ("focus",)

    def _make_meta(self, group: Group) -> FocusGroupMeta:
        """Create metadata for focus group."""
        if not group.items or not group.items[0].meta:
            master = "toltec"
            obsnum_start = 0
            obsnum_end = 0
        else:
            first = group.items[0].meta
            last = group.items[-1].meta
            master = first.master or "toltec"
            obsnum_start = first.obsnum or 0
            obsnum_end = last.obsnum or obsnum_start
        
        n_items = len(group.items)
        
        # Name includes obsnum range for consecutive groups
        if obsnum_start == obsnum_end:
            name = f"{master}-{obsnum_start}-g{n_items}-focus"
        else:
            name = f"{master}-{obsnum_start}to{obsnum_end}-g{n_items}-focus"
        
        return FocusGroupMeta(
            name=name,
            data_prod_type=DataProdType.DP_FOCUS_GROUP,
            master=master,
            obsnum=obsnum_start,
            n_items=n_items,
        )


class AstigmatismGroupCollator(CollateByConsecutiveObsnum):
    """Collator for astigmatism measurement sequences.
    
    Identifies groups of consecutive observations with obs_goal='astig' or 'astigmatism'.
    These are used to measure and correct for telescope astigmatism.
    
    Groups are formed from consecutive obsnums (e.g., 145650, 145651, 145652).
    A gap in obsnum sequence starts a new group.
    """

    data_prod_type: ClassVar[str] = DataProdType.DP_ASTIG_GROUP.value
    data_prod_assoc_type: ClassVar[str] = DataProdAssocType.DPA_ASTIG_GROUP_RAW_OBS.value
    obs_goal_filter: ClassVar[tuple[str, ...]] = ("astig", "astigmatism")

    def _make_meta(self, group: Group) -> AstigGroupMeta:
        """Create metadata for astigmatism group."""
        if not group.items or not group.items[0].meta:
            master = "toltec"
            obsnum_start = 0
            obsnum_end = 0
        else:
            first = group.items[0].meta
            last = group.items[-1].meta
            master = first.master or "toltec"
            obsnum_start = first.obsnum or 0
            obsnum_end = last.obsnum or obsnum_start
        
        n_items = len(group.items)
        
        # Name includes obsnum range for consecutive groups
        if obsnum_start == obsnum_end:
            name = f"{master}-{obsnum_start}-g{n_items}-astig"
        else:
            name = f"{master}-{obsnum_start}to{obsnum_end}-g{n_items}-astig"
        
        return AstigGroupMeta(
            name=name,
            data_prod_type=DataProdType.DP_ASTIG_GROUP,
            master=master,
            obsnum=obsnum_start,
            n_items=n_items,
        )
