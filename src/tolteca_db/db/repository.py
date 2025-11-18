"""Repository pattern for data access layer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.orm import DeclarativeBase as SQLModel

__all__ = [
    "BaseRepository",
    "DataProductRepository",
    "FlagDefinitionRepository",
    "DataProductFlagRepository",
]

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
        return list(self.session.execute(stmt).scalars().all())

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
            self.model_class.name == name,
        )
        return self.session.execute(stmt).scalars().first()

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
            self.model_class.availability_state == "AVAILABLE",
        )

        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)

        # Exclude products with BLOCK/CRITICAL flags
        # NOT EXISTS subquery pattern
        blocking_flags_subquery = (
            select(DataProductFlag.product_fk)
            .join(FlagDefinition)
            .where(
                DataProductFlag.product_fk == self.model_class.product_pk,
                FlagDefinition.severity.in_(["BLOCK", "CRITICAL"]),
                FlagDefinition.active == True,  # noqa: E712
            )
        )
        stmt = stmt.where(~self.model_class.product_pk.in_(blocking_flags_subquery))

        return list(self.session.execute(stmt).scalars().all())

    def supersede_reduced(self, product_pk: str) -> Any:
        """
        Mark REDUCED product as SUPERSEDED.

        Parameters
        ----------
        product_pk : str
            Product primary key to supersede

        Returns
        -------
        DataProduct
            Updated product

        Raises
        ------
        ValueError
            If product not found or not REDUCED
        """
        product = self.get(product_pk)
        if not product:
            msg = f"Product {product_pk} not found"
            raise ValueError(msg)

        if product.product_kind != "REDUCED":
            msg = f"Product {product_pk} is not REDUCED"
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
            self.model_class.status == "ACTIVE",
        )

        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)

        return list(self.session.execute(stmt).scalars().all())

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
        >>> product = repo.get_with_storage("product_pk_123")
        >>> for storage in product.storage_locations:  # No extra query
        ...     print(storage.storage_key)
        """
        stmt = (
            select(self.model_class)
            .where(self.model_class.product_pk == id_value)
            .options(selectinload(self.model_class.storage_locations))
        )
        return self.session.execute(stmt).scalars().first()

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
            .where(self.model_class.product_pk == id_value)
            .options(selectinload(self.model_class.flags))
        )
        return self.session.execute(stmt).scalars().first()

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
            .where(self.model_class.product_pk == id_value)
            .options(
                selectinload(self.model_class.storage_locations),
                selectinload(self.model_class.flags),
            )
        )
        return self.session.execute(stmt).scalars().first()

    def list_with_storage(
        self,
        base_type: str | None = None,
        limit: int | None = None,
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
            selectinload(self.model_class.storage_locations),
        )

        if base_type:
            stmt = stmt.where(self.model_class.base_type == base_type)

        if limit:
            stmt = stmt.limit(limit)

        return list(self.session.execute(stmt).scalars().all())


class FlagDefinitionRepository(BaseRepository):
    """
    Repository for FlagDefinition operations.

    Manages flag definitions with lookup by name and severity filtering.
    """

    def get_by_key(self, flag_key: str) -> Any:
        """
        Get flag definition by key.

        Parameters
        ----------
        flag_key : str
            Flag key (e.g., DET_DEAD_PIXEL)

        Returns
        -------
        FlagDefinition | None
            Flag definition or None if not found
        """
        stmt = select(self.model_class).where(self.model_class.flag_key == flag_key)
        return self.session.execute(stmt).scalars().first()

    def list_by_severity(self, severity: str) -> list[Any]:
        """
        List flag definitions by severity level.

        Parameters
        ----------
        severity : str
            Severity level (INFO, WARN, BLOCK, CRITICAL)

        Returns
        -------
        list[FlagDefinition]
            Flag definitions matching severity
        """
        stmt = select(self.model_class).where(self.model_class.severity == severity)
        return list(self.session.execute(stmt).scalars().all())

    def create_from_schema(self, schema: Any) -> Any:
        """
        Create flag definition from Pydantic schema.

        Parameters
        ----------
        schema : FlagDefinitionCreate
            Pydantic schema with validated data

        Returns
        -------
        FlagDefinition
            Created flag definition
        """
        from tolteca_db.models.orm import FlagDefinition

        # flag_key is the primary key - use directly from schema
        flag_def = FlagDefinition(**schema.model_dump())

        return self.create(flag_def)


class DataProductFlagRepository(BaseRepository):
    """
    Repository for DataProductFlag operations.

    Manages flags applied to data products with filtering by severity and status.
    """

    def list_by_product(self, product_fk: str) -> list[Any]:
        """
        List all flags for a product.

        Parameters
        ----------
        product_fk : str
            Product foreign key

        Returns
        -------
        list[DataProductFlag]
            Flags applied to the product
        """
        stmt = select(self.model_class).where(
            self.model_class.product_fk == product_fk
        )
        return list(self.session.execute(stmt).scalars().all())

    def list_by_severity(self, product_fk: str, severity: str) -> list[Any]:
        """
        List flags for a product filtered by severity.

        Parameters
        ----------
        product_fk : str
            Product foreign key
        severity : str
            Severity level to filter

        Returns
        -------
        list[DataProductFlag]
            Flags matching criteria
        """
        from tolteca_db.models import FlagDefinition

        stmt = (
            select(self.model_class)
            .join(FlagDefinition)
            .where(
                self.model_class.product_fk == product_fk,
                FlagDefinition.severity == severity,
            )
        )
        return list(self.session.execute(stmt).scalars().all())

    def create_from_schema(self, schema: Any) -> Any:
        """
        Create data product flag from Pydantic schema.

        Parameters
        ----------
        schema : DataProductFlagCreate
            Pydantic schema with validated data

        Returns
        -------
        DataProductFlag
            Created flag
        """
        from tolteca_db.models.orm import DataProductFlag

        # Composite PK (product_fk, flag_key) - use directly from schema
        flag = DataProductFlag(**schema.model_dump())

        return self.create(flag)
