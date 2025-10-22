"""Repository pattern for data access layer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

if TYPE_CHECKING:
    from typing import Any
    from sqlmodel import SQLModel

__all__ = ["BaseRepository", "DataProductRepository"]

T = TypeVar("T", bound="SQLModel")


class BaseRepository(Generic[T]):
    """
    Base repository providing CRUD operations.

    Parameters
    ----------
    session : Session
        SQLModel database session
    model_class : type[T]
        SQLModel model class

    Examples
    --------
    >>> from tolteca_db.models import DataProduct
    >>> repo = BaseRepository(session, DataProduct)
    >>> product = repo.get("product_id_123")
    """

    def __init__(self, session: Session, model_class: type[T]) -> None:
        """Initialize repository."""
        self.session = session
        self.model_class = model_class

    def get(self, id_value: Any) -> T | None:
        """
        Get entity by primary key.

        Parameters
        ----------
        id_value : Any
            Primary key value

        Returns
        -------
        T | None
            Entity instance or None if not found
        """
        return self.session.get(self.model_class, id_value)

    def list(self, **filters: Any) -> list[T]:
        """
        List entities with optional filters.

        Parameters
        ----------
        **filters : Any
            Field=value filters

        Returns
        -------
        list[T]
            List of matching entities

        Examples
        --------
        >>> products = repo.list(base_type="raw_obs")
        """
        stmt = select(self.model_class)
        for key, value in filters.items():
            stmt = stmt.where(getattr(self.model_class, key) == value)
        return list(self.session.exec(stmt).all())

    def create(self, obj: T) -> T:
        """
        Create new entity.

        Parameters
        ----------
        obj : T
            Entity instance

        Returns
        -------
        T
            Created entity (with DB-generated fields populated)
        """
        self.session.add(obj)
        self.session.flush()
        self.session.refresh(obj)
        return obj

    def update(self, obj: T) -> T:
        """
        Update existing entity.

        Parameters
        ----------
        obj : T
            Entity instance with modifications

        Returns
        -------
        T
            Updated entity
        """
        self.session.add(obj)
        self.session.flush()
        self.session.refresh(obj)
        return obj

    def delete(self, obj: T) -> None:
        """
        Delete entity.

        Parameters
        ----------
        obj : T
            Entity instance to delete
        """
        self.session.delete(obj)
        self.session.flush()


class DataProductRepository(BaseRepository):
    """
    Specialized repository for DataProduct operations.

    Extends BaseRepository with product-specific queries for RAW/REDUCED
    lifecycle management, usability filtering, and provenance.

    Examples
    --------
    >>> from tolteca_db.models import DataProduct
    >>> repo = DataProductRepository(session, DataProduct)
    >>> usable = repo.list_raw_usable(base_type="raw_obs")
    """

    def get_by_name(self, base_type: str, name: str) -> Any:
        """
        Get product by (base_type, name) unique constraint.

        Parameters
        ----------
        base_type : str
            Product base type
        name : str
            Product name

        Returns
        -------
        DataProduct | None
            Product instance or None
        """
        stmt = select(self.model_class).where(
            self.model_class.base_type == base_type,
            self.model_class.name == name
        )
        return self.session.exec(stmt).first()

    def list_raw_usable(self, base_type: str | None = None) -> list[Any]:
        """
        List RAW products that are usable.

        Filters out products with BLOCK/CRITICAL flags or MISSING availability.

        Parameters
        ----------
        base_type : str | None, optional
            Filter by base_type

        Returns
        -------
        list[DataProduct]
            List of usable RAW products

        Notes
        -----
        Uses NOT EXISTS subquery to filter products with blocking flags.
        See viscore repository.py for implementation pattern.
        """
        from tolteca_db.models import DataProductFlag, FlagDefinition
        
        # Start with RAW products that are AVAILABLE
        stmt = select(self.model_class).where(
            self.model_class.product_kind == "RAW",
            self.model_class.availability_state == "AVAILABLE"
        )
        
        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)
        
        # Exclude products with BLOCK/CRITICAL flags
        # NOT EXISTS subquery pattern
        blocking_flags_subquery = (
            select(DataProductFlag.product_id)
            .join(FlagDefinition)
            .where(
                DataProductFlag.product_id == self.model_class.product_id,
                FlagDefinition.severity.in_(["BLOCK", "CRITICAL"]),
                FlagDefinition.active == True  # noqa: E712
            )
        )
        stmt = stmt.where(~self.model_class.product_id.in_(blocking_flags_subquery))
        
        return list(self.session.exec(stmt).all())

    def supersede_reduced(self, product_id: str) -> Any:
        """
        Mark REDUCED product as SUPERSEDED.

        Parameters
        ----------
        product_id : str
            Product ID to supersede

        Returns
        -------
        DataProduct
            Updated product

        Raises
        ------
        ValueError
            If product not found or not REDUCED
        """
        product = self.get(product_id)
        if not product:
            msg = f"Product {product_id} not found"
            raise ValueError(msg)
        
        if product.product_kind != "REDUCED":
            msg = f"Product {product_id} is not REDUCED"
            raise ValueError(msg)
        
        product.status = "SUPERSEDED"
        return self.update(product)

    def list_reduced_active(self, base_type: str | None = None) -> list[Any]:
        """
        List ACTIVE reduced products.

        Parameters
        ----------
        base_type : str | None, optional
            Filter by base_type

        Returns
        -------
        list[DataProduct]
            List of ACTIVE reduced products
        """
        stmt = select(self.model_class).where(
            self.model_class.product_kind == "REDUCED",
            self.model_class.status == "ACTIVE"
        )
        
        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)
        
        return list(self.session.exec(stmt).all())

    def get_with_storage(self, id_value: Any) -> Any:
        """
        Get product with storage locations eagerly loaded.

        Prevents N+1 queries when accessing storage_locations relationship.

        Parameters
        ----------
        id_value : Any
            Primary key value

        Returns
        -------
        DataProduct | None
            Product with storage_locations loaded, or None

        Examples
        --------
        >>> product = repo.get_with_storage("product_id_123")
        >>> for storage in product.storage_locations:  # No extra query
        ...     print(storage.storage_key)
        """
        stmt = (
            select(self.model_class)
            .where(self.model_class.product_id == id_value)
            .options(selectinload(self.model_class.storage_locations))
        )
        return self.session.exec(stmt).first()

    def get_with_flags(self, id_value: Any) -> Any:
        """
        Get product with flags eagerly loaded.

        Prevents N+1 queries when accessing flags relationship.

        Parameters
        ----------
        id_value : Any
            Primary key value

        Returns
        -------
        DataProduct | None
            Product with flags loaded, or None
        """
        stmt = (
            select(self.model_class)
            .where(self.model_class.product_id == id_value)
            .options(selectinload(self.model_class.flags))
        )
        return self.session.exec(stmt).first()

    def get_with_all_relations(self, id_value: Any) -> Any:
        """
        Get product with all relationships eagerly loaded.

        Loads storage_locations and flags in single query for complete product view.

        Parameters
        ----------
        id_value : Any
            Primary key value

        Returns
        -------
        DataProduct | None
            Product with all relationships loaded, or None

        Notes
        -----
        Use when you need access to multiple relationships to minimize queries.
        For single relationship access, use get_with_storage() or get_with_flags().
        """
        stmt = (
            select(self.model_class)
            .where(self.model_class.product_id == id_value)
            .options(
                selectinload(self.model_class.storage_locations),
                selectinload(self.model_class.flags)
            )
        )
        return self.session.exec(stmt).first()

    def list_with_storage(
        self,
        base_type: str | None = None,
        limit: int | None = None
    ) -> list[Any]:
        """
        List products with storage locations eagerly loaded.

        Prevents N+1 queries when iterating over products and accessing storage.

        Parameters
        ----------
        base_type : str | None, optional
            Filter by base_type
        limit : int | None, optional
            Maximum number of results

        Returns
        -------
        list[DataProduct]
            Products with storage_locations loaded

        Examples
        --------
        >>> products = repo.list_with_storage(base_type="raw_obs", limit=100)
        >>> for product in products:
        ...     for storage in product.storage_locations:  # No N+1 queries
        ...         print(storage.location_id)
        """
        stmt = select(self.model_class).options(
            selectinload(self.model_class.storage_locations)
        )
        
        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)
        
        if limit:
            stmt = stmt.limit(limit)
        
        return list(self.session.exec(stmt).all())
