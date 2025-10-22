"""NetCDF file I/O handler for TolTEC raw data."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from tolteca_db.io import FileIOBase

if TYPE_CHECKING:
    pass


class NetCDFHandler(FileIOBase):
    """
    Handler for NetCDF files (TolTEC raw observation data).

    Handles .nc files containing TolTEC timestream and housekeeping data.
    """

    format_name: ClassVar[str] = "netcdf"
    extensions: ClassVar[list[str]] = [".nc"]

    def can_handle(self, file_path: Path) -> bool:
        """
        Check if file is a valid NetCDF file.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if file has .nc extension and exists
        """
        return file_path.suffix.lower() == ".nc" and file_path.exists()

    def extract_metadata(self, file_path: Path) -> dict[str, Any]:
        """
        Extract metadata from NetCDF file.

        Parameters
        ----------
        file_path : Path
            Path to NetCDF file

        Returns
        -------
        dict[str, Any]
            Metadata including dimensions, variables, and global attributes
        """
        try:
            import netCDF4  # type: ignore[import]

            with netCDF4.Dataset(file_path, "r") as nc:
                metadata = {
                    "format": "netcdf",
                    "dimensions": {dim: len(nc.dimensions[dim]) for dim in nc.dimensions},
                    "variables": list(nc.variables.keys()),
                    "attributes": {k: nc.getncattr(k) for k in nc.ncattrs()},
                }
                return metadata

        except (ImportError, OSError):
            # netCDF4 not available or file can't be opened
            return {
                "format": "netcdf",
                "error": "Cannot read NetCDF file",
            }

    def validate(self, file_path: Path) -> bool:
        """
        Validate NetCDF file integrity.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if file can be opened
        """
        try:
            import netCDF4  # type: ignore[import]

            with netCDF4.Dataset(file_path, "r"):
                return True

        except (ImportError, OSError):
            return False
