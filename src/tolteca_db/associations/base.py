"""Base collator classes for grouping observations.

Collators analyze sequences of raw observations and identify groups that should
be associated together (calibration sequences, drive fits, focus runs, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Flag, auto
from typing import ClassVar

from tolteca_db.constants import ToltecDataKind

__all__ = [
    "CollatorBase",
    "GroupFlag",
    "Group",
    "Position",
]


class GroupFlag(Flag):
    """Flags indicating how a group was formed."""

    IMPLICIT = auto()
    EXPLICIT_START = auto()
    EXPLICIT_END = auto()
    EXPLICIT = EXPLICIT_START | EXPLICIT_END
    IMPLICIT_START = IMPLICIT | EXPLICIT_END
    IMPLICIT_END = IMPLICIT | EXPLICIT_START


class Position(Flag):
    """Position of an observation within a sequence."""

    START = auto()
    END = auto()
    INNER = auto()
    NOT_START = INNER | END
    NOT_END = START | INNER
    OUTER = START | END
    ANYWHERE = INNER | OUTER


@dataclass
class Group:
    """A group of related observations.
    
    Attributes
    ----------
    flag : GroupFlag
        How this group was identified
    items : list
        DataProd instances in this group
    meta : dict
        Additional metadata about the group
    """

    flag: GroupFlag = GroupFlag.IMPLICIT
    items: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    def append(self, item, add_flag: GroupFlag | None = None):
        """Add an item to the group."""
        if add_flag is not None:
            self.flag |= add_flag
        self.items.append(item)


@dataclass
class AssociationInfo:
    """Information about an association between observations.
    
    Attributes
    ----------
    data_prod_assoc_type : str
        Type of association (e.g., 'dpa_cal_group_obs')
    data_prod_pk : int
        Primary key of the associated observation
    """

    data_prod_assoc_type: str
    data_prod_pk: int


class CollatorBase(ABC):
    """Base class for observation collators.
    
    Collators identify groups of related observations and create associations
    between them. Each collator implements specific grouping logic for different
    types of observation sequences (calibration, drive fits, focus runs, etc.).
    """

    # Subclasses set these
    data_prod_type: ClassVar[str]
    data_prod_assoc_type: ClassVar[str]

    @abstractmethod
    def make_groups(self, observations: list) -> list[Group]:
        """Identify groups within a list of observations.
        
        Parameters
        ----------
        observations : list
            List of DataProd ORM instances to analyze
            
        Returns
        -------
        list[Group]
            List of identified groups
        """
        ...

    def make_associations(self, observations: list) -> list[tuple[dict, list[AssociationInfo]]]:
        """Create associations for grouped observations.
        
        Parameters
        ----------
        observations : list
            List of DataProd ORM instances to analyze
            
        Returns
        -------
        list[tuple[dict, list[AssociationInfo]]]
            List of (group_metadata, associations) tuples
        """
        groups = self.make_groups(observations)
        results = []
        
        for group in groups:
            # Create metadata for the group
            meta = self._make_meta(group)
            
            # Create associations for each item in the group
            assocs = [
                AssociationInfo(
                    data_prod_assoc_type=self.data_prod_assoc_type,
                    data_prod_pk=item.pk,
                )
                for item in group.items
            ]
            
            results.append((meta, assocs))
        
        return results

    @abstractmethod
    def _make_meta(self, group: Group) -> Any:
        """Create metadata dataclass for a group.
        
        Parameters
        ----------
        group : Group
            Group to create metadata for
            
        Returns
        -------
        Any
            Metadata dataclass (CalGroupMeta, DrivefitMeta, etc.)
        """
        ...
