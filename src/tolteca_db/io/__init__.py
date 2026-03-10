"""File I/O registry for data products."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    pass

__all__ = ["FileIOBase", "IORegistry"]


class FileIOBase(ABC):
    """
    Abstract base class for file I/O handlers.

    Each subclass handles a specific file format (FITS, ECSV, NetCDF, etc.)
    and provides methods for metadata extraction and validation.
    """

    format_name: ClassVar[str]
    extensions: ClassVar[list[str]]

    @abstractmethod
    def can_handle(self, file_path: Path) -> bool:
        """
        Check if this handler can process the file.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if handler can process this file
        """
        ...

    @abstractmethod
    def extract_metadata(self, file_path: Path) -> dict[str, Any]:
        """
        Extract metadata from file.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        dict[str, Any]
            Extracted metadata
        """
        ...

    @abstractmethod
    def validate(self, file_path: Path) -> bool:
        """
        Validate file integrity.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if file is valid
        """
        ...


class IORegistry:
    """
    Registry for file I/O handlers.

    Automatically selects the appropriate handler based on file extension
    or file content inspection.

    Examples
    --------
    >>> registry = IORegistry()
    >>> registry.register(FITSHandler())
    >>> registry.register(NetCDFHandler())
    >>> handler = registry.get_handler("data.fits")
    >>> metadata = handler.extract_metadata(Path("data.fits"))
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._handlers: dict[str, FileIOBase] = {}
        self._extension_map: dict[str, str] = {}

    def register(self, handler: FileIOBase) -> None:
        """
        Register a file I/O handler.

        Parameters
        ----------
        handler : FileIOBase
            Handler instance to register
        """
        format_name = handler.format_name
        self._handlers[format_name] = handler

        # Build extension mapping
        for ext in handler.extensions:
            self._extension_map[ext.lower()] = format_name

    def get_handler(self, file_path: str | Path) -> FileIOBase | None:
        """
        Get appropriate handler for file.

        First tries extension-based lookup, then falls back to
        content inspection via can_handle().

        Parameters
        ----------
        file_path : str | Path
            Path to file

        Returns
        -------
        FileIOBase | None
            Handler instance or None if no handler found
        """
        path = Path(file_path)

        # Try extension-based lookup
        ext = path.suffix.lower()
        if ext in self._extension_map:
            format_name = self._extension_map[ext]
            return self._handlers[format_name]

        # Fall back to content inspection
        for handler in self._handlers.values():
            if handler.can_handle(path):
                return handler

        return None

    def list_formats(self) -> list[str]:
        """
        List registered format names.

        Returns
        -------
        list[str]
            Format names
        """
        return list(self._handlers.keys())

    def list_extensions(self) -> list[str]:
        """
        List all supported file extensions.

        Returns
        -------
        list[str]
            File extensions
        """
        return list(self._extension_map.keys())


# Global registry instance
_global_registry: IORegistry | None = None


def get_registry() -> IORegistry:
    """
    Get the global IO registry instance.

    Returns
    -------
    IORegistry
        Singleton registry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = IORegistry()
    return _global_registry
