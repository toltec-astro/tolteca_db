"""Association generator for creating data product relationships.

This module provides the main AssociationGenerator class that queries
observations, applies collators, and creates association entries in the database.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from tolteca_db.models.metadata import CalGroupMeta, DrivefitMeta, FocusGroupMeta
from tolteca_db.models.orm import DataProd, DataProdAssoc, DataProdAssocType, DataProdType

from .collators import CalGroupCollator, DriveFitCollator, FocusGroupCollator
from .pool import ObservationPool
from .state import AssociationState, GroupInfo

__all__ = ["AssociationGenerator", "AssociationStats"]


@dataclass
class AssociationStats:
    """Statistics from association generation.
    
    Attributes
    ----------
    observations_scanned : int
        Number of observations analyzed
    observations_already_grouped : int
        Number of observations already in groups (incremental mode)
    observations_processed : int
        Number of observations actually processed (scanned - already_grouped)
    groups_created : int
        Number of groups identified
    groups_updated : int
        Number of existing groups updated with new members (incremental mode)
    associations_created : int
        Number of association entries created
    cal_groups : int
        Number of calibration groups
    drivefit_groups : int
        Number of drivefit groups
    focus_groups : int
        Number of focus groups
    """

    observations_scanned: int = 0
    observations_already_grouped: int = 0
    observations_processed: int = 0
    groups_created: int = 0
    groups_updated: int = 0
    associations_created: int = 0
    cal_groups: int = 0
    drivefit_groups: int = 0
    focus_groups: int = 0


class AssociationGenerator:
    """Generate associations between related observations.
    
    This class queries raw observations from the database and applies collators
    to identify groups that should be associated together (calibration sequences,
    drive fits, focus runs, etc.).
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session for database access
    
    Examples
    --------
    >>> from tolteca_db.db import get_engine
    >>> from sqlalchemy.orm import Session
    >>> 
    >>> engine = get_engine("duckdb:///path/to/db.duckdb")
    >>> with Session(engine) as session:
    ...     generator = AssociationGenerator(session)
    ...     stats = generator.generate_associations(n_observations=100)
    ...     print(f"Created {stats.groups_created} groups")
    """

    def __init__(self, session: Session, state: AssociationState | None = None):
        self.session = session
        self.state = state
        
        # Initialize collators
        self.collators = [
            CalGroupCollator(),
            DriveFitCollator(),
            FocusGroupCollator(),
        ]
        
        # Pre-fetch type PKs for efficiency
        self._dp_type_pks = self._get_type_pks()
        self._assoc_type_pks = self._get_assoc_type_pks()

    def _get_type_pks(self) -> dict[str, int]:
        """Get primary keys for all data product types."""
        stmt = select(DataProdType)
        results = self.session.execute(stmt).scalars().all()
        return {dp_type.label: dp_type.pk for dp_type in results}

    def _get_assoc_type_pks(self) -> dict[str, int]:
        """Get primary keys for all association types."""
        stmt = select(DataProdAssocType)
        results = self.session.execute(stmt).scalars().all()
        return {assoc_type.label: assoc_type.pk for assoc_type in results}

    def generate_associations(
        self,
        n_observations: int | None = None,
        location_pk: int | None = None,
        commit: bool = True,
        incremental: bool = False,
    ) -> AssociationStats:
        """Generate associations for recent observations.
        
        Parameters
        ----------
        n_observations : int, optional
            Number of recent observations to analyze.
            If None, analyzes all observations.
        location_pk : int, optional
            Filter to observations at specific location
        commit : bool, default=True
            Whether to commit changes to database
        incremental : bool, default=False
            If True and state is available, skip already-grouped observations
            and update existing groups. Requires self.state to be set.
            
        Returns
        -------
        AssociationStats
            Statistics about associations created
        """
        # Query recent raw observations
        observations = self._query_observations(
            n_observations=n_observations,
            location_pk=location_pk,
        )
        
        return self.generate_from_batch(
            observations=observations,
            commit=commit,
            incremental=incremental,
        )

    def _query_observations(
        self,
        n_observations: int | None = None,
        location_pk: int | None = None,
    ) -> list[DataProd]:
        """Query raw observations from database.
        
        Parameters
        ----------
        n_observations : int, optional
            Number of recent observations to fetch
        location_pk : int, optional
            Filter to specific location
            
        Returns
        -------
        list[DataProd]
            List of observations sorted by time
        """
        # Query raw observations (data_prod_type_fk = 1)
        dp_raw_obs_pk = self._dp_type_pks.get("dp_raw_obs", 1)
        
        stmt = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == dp_raw_obs_pk)
            .order_by(DataProd.pk.desc())
        )
        
        # Filter by location if specified
        if location_pk is not None:
            # Join with DataProdSource to filter by location
            from tolteca_db.models.orm import DataProdSource
            stmt = stmt.join(DataProdSource).where(
                DataProdSource.location_fk == location_pk
            )
        
        # Limit if specified
        if n_observations is not None:
            stmt = stmt.limit(n_observations)
        
        # Execute and reverse to chronological order
        results = list(self.session.execute(stmt).scalars().all())
        results.reverse()
        
        return results

    def _process_collator(self, collator, observations: list[DataProd]) -> dict:
        """Apply a collator and create associations.
        
        Parameters
        ----------
        collator : CollatorBase
            Collator to apply
        observations : list[DataProd]
            Observations to analyze
            
        Returns
        -------
        dict
            Statistics: {'groups': int, 'assocs': int}
        """
        groups_created = 0
        assocs_created = 0
        
        # Get associations from collator
        results = collator.make_associations(observations)
        
        for group_meta, assoc_infos in results:
            # Create the group data product
            group_dp = self._create_group_data_prod(
                collator=collator,
                meta=group_meta,
            )
            groups_created += 1
            
            # Create association entries
            for assoc_info in assoc_infos:
                self._create_association(
                    group_dp_pk=group_dp.pk,
                    obs_dp_pk=assoc_info.data_prod_pk,
                    assoc_type_label=assoc_info.data_prod_assoc_type,
                )
                assocs_created += 1
        
        return {"groups": groups_created, "assocs": assocs_created}

    def _create_group_data_prod(self, collator, meta: dict) -> DataProd:
        """Create a DataProd entry for a group.
        
        Parameters
        ----------
        collator : CollatorBase
            Collator that created the group
        meta : dict
            Metadata for the group
            
        Returns
        -------
        DataProd
            Created group data product
        """
        # Get the data product type PK
        dp_type_pk = self._dp_type_pks[collator.data_prod_type]
        
        # Create DataProd entry
        group_dp = DataProd(
            data_prod_type_fk=dp_type_pk,
            meta=meta,  # Store as JSON
        )
        
        self.session.add(group_dp)
        self.session.flush()  # Get PK assigned
        
        return group_dp

    def _create_association(
        self,
        group_dp_pk: int,
        obs_dp_pk: int,
        assoc_type_label: str,
    ) -> DataProdAssoc:
        """Create an association entry.
        
        Parameters
        ----------
        group_dp_pk : int
            Primary key of the group data product (source)
        obs_dp_pk : int
            Primary key of the observation data product (destination)
        assoc_type_label : str
            Label of the association type
            
        Returns
        -------
        DataProdAssoc
            Created association entry
        """
        assoc_type_pk = self._assoc_type_pks[assoc_type_label]
        
        assoc = DataProdAssoc(
            data_prod_assoc_type_fk=assoc_type_pk,
            src_data_prod_fk=group_dp_pk,
            dst_data_prod_fk=obs_dp_pk,
        )
        
        self.session.add(assoc)
        return assoc

    def generate_from_batch(
        self,
        observations: list[DataProd],
        commit: bool = True,
        incremental: bool = False,
    ) -> AssociationStats:
        """Generate associations from a batch of pre-loaded observations.
        
        Parameters
        ----------
        observations : list[DataProd]
            Pre-loaded observations to process
        commit : bool, default=True
            Whether to commit changes to database
        incremental : bool, default=False
            If True and state is available, skip already-grouped observations
            
        Returns
        -------
        AssociationStats
            Statistics about associations created
        """
        stats = AssociationStats()
        stats.observations_scanned = len(observations)
        
        if not observations:
            return stats
        
        # Create pool for fast filtering
        pool = ObservationPool(observations)
        
        # Filter to ungrouped observations if incremental mode
        if incremental and self.state is not None:
            obs_pks = pool.data["pk"].tolist()
            ungrouped_pks = self.state.get_ungrouped(obs_pks)
            stats.observations_already_grouped = len(obs_pks) - len(ungrouped_pks)
            
            # Filter pool to ungrouped only
            pool.data = pool.data[pool.data["pk"].isin(ungrouped_pks)]
        
        stats.observations_processed = len(pool)
        
        if len(pool) == 0:
            return stats
        
        # Apply each collator
        for collator in self.collators:
            if incremental and self.state is not None:
                group_stats = self._process_collator_incremental(collator, pool)
            else:
                # Convert pool back to list for non-incremental processing
                obs_list = pool.get_observations(pool.data["pk"].tolist())
                group_stats = self._process_collator(collator, obs_list)
            
            stats.groups_created += group_stats["groups"]
            stats.groups_updated += group_stats.get("updated", 0)
            stats.associations_created += group_stats["assocs"]
            
            # Track by type
            if isinstance(collator, CalGroupCollator):
                stats.cal_groups = group_stats["groups"] + group_stats.get("updated", 0)
            elif isinstance(collator, DriveFitCollator):
                stats.drivefit_groups = group_stats["groups"] + group_stats.get("updated", 0)
            elif isinstance(collator, FocusGroupCollator):
                stats.focus_groups = group_stats["groups"] + group_stats.get("updated", 0)
        
        # Flush state if incremental
        if incremental and self.state is not None:
            self.state.flush()
        
        if commit:
            self.session.commit()
        
        return stats

    def generate_streaming(
        self,
        observation_iterator,
        batch_size: int = 100,
        commit_every: int = 1,
        incremental: bool = True,
    ):
        """Generate associations from streaming observations.
        
        Parameters
        ----------
        observation_iterator : Iterator[DataProd]
            Iterator yielding observations
        batch_size : int, default=100
            Number of observations per batch
        commit_every : int, default=1
            Commit every N batches
        incremental : bool, default=True
            Enable incremental processing
            
        Yields
        ------
        AssociationStats
            Statistics for each batch processed
        """
        batch = []
        batch_count = 0
        
        for obs in observation_iterator:
            batch.append(obs)
            
            if len(batch) >= batch_size:
                # Process batch
                stats = self.generate_from_batch(
                    observations=batch,
                    commit=False,
                    incremental=incremental,
                )
                
                batch_count += 1
                
                # Commit if needed
                if batch_count % commit_every == 0:
                    self.session.commit()
                
                yield stats
                batch = []
        
        # Process remaining observations
        if batch:
            stats = self.generate_from_batch(
                observations=batch,
                commit=True,
                incremental=incremental,
            )
            yield stats

    def _process_collator_incremental(
        self,
        collator,
        pool: ObservationPool,
    ) -> dict:
        """Apply collator with incremental state tracking.
        
        Parameters
        ----------
        collator : CollatorBase
            Collator to apply
        pool : ObservationPool
            Pool of observations to analyze
            
        Returns
        -------
        dict
            Statistics: {'groups': int, 'updated': int, 'assocs': int}
        """
        groups_created = 0
        groups_updated = 0
        assocs_created = 0
        
        # Get observations from pool
        observations = pool.get_observations(pool.data["pk"].tolist())
        
        # Get associations from collator
        results = collator.make_associations(observations)
        
        for group_meta, assoc_infos in results:
            # Build candidate key for this group
            candidate_key = self._build_candidate_key(collator, group_meta)
            
            # Check if group already exists
            existing_group = self.state.get_existing_group(candidate_key)
            
            if existing_group:
                # Update existing group
                new_member_pks = [info.data_prod_pk for info in assoc_infos]
                self._update_existing_group(
                    existing_group=existing_group,
                    new_member_pks=new_member_pks,
                )
                groups_updated += 1
            else:
                # Create new group
                group_dp = self._create_group_data_prod(
                    collator=collator,
                    meta=group_meta,
                )
                groups_created += 1
                
                # Register in state
                group_info = GroupInfo(
                    group_pk=group_dp.pk,
                    group_type=collator.data_prod_type,
                    candidate_key=candidate_key,
                    n_members=len(assoc_infos),
                    metadata=group_meta,
                )
                self.state.register_group(group_info)
            
            # Create association entries for new members
            for assoc_info in assoc_infos:
                obs_pk = assoc_info.data_prod_pk
                
                # Only create association if not already grouped
                if not self.state.is_grouped(obs_pk):
                    group_pk = existing_group.group_pk if existing_group else group_dp.pk
                    self._create_association(
                        group_dp_pk=group_pk,
                        obs_dp_pk=obs_pk,
                        assoc_type_label=assoc_info.data_prod_assoc_type,
                    )
                    assocs_created += 1
                    
                    # Mark as grouped
                    self.state.mark_grouped(obs_pk)
        
        return {"groups": groups_created, "updated": groups_updated, "assocs": assocs_created}

    def _build_candidate_key(self, collator, meta: dict) -> str:
        """Build candidate key for group identification.
        
        Parameters
        ----------
        collator : CollatorBase
            Collator that created the group
        meta : dict
            Group metadata
            
        Returns
        -------
        str
            Unique candidate key
        """
        # Build key from group type and identifying metadata
        parts = [collator.data_prod_type]
        
        # Add identifying fields based on group type
        # Handle both dict and dataclass metadata
        if isinstance(collator, CalGroupCollator):
            obsnum = meta.get("obsnum", "") if isinstance(meta, dict) else getattr(meta, "obsnum", "")
            master = meta.get("master", "") if isinstance(meta, dict) else getattr(meta, "master", "")
            parts.append(str(obsnum))
            parts.append(str(master))
        elif isinstance(collator, DriveFitCollator):
            obsnum = meta.get("obsnum", "") if isinstance(meta, dict) else getattr(meta, "obsnum", "")
            master = meta.get("master", "") if isinstance(meta, dict) else getattr(meta, "master", "")
            parts.append(str(obsnum))
            parts.append(str(master))
        elif isinstance(collator, FocusGroupCollator):
            obsnum = meta.get("obsnum", "") if isinstance(meta, dict) else getattr(meta, "obsnum", "")
            parts.append(str(obsnum))
        
        return "_".join(parts)

    def _update_existing_group(
        self,
        existing_group: GroupInfo,
        new_member_pks: list[int],
    ) -> None:
        """Add new members to an existing group.
        
        Parameters
        ----------
        existing_group : GroupInfo
            Information about the existing group
        new_member_pks : list[int]
            Primary keys of new members to add
        """
        # Update member count in state
        new_count = existing_group.n_members + len(new_member_pks)
        self.state.update_group_member_count(existing_group.candidate_key, new_count)
