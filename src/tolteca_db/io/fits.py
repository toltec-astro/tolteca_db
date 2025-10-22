"""FITS file I/O handler."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from tolteca_db.io import FileIOBase

if TYPE_CHECKING:
    pass


class FITSHandler(FileIOBase):
    """
    Handler for FITS files.

    Handles Flexible Image Transport System files commonly used
    in astronomy for images and tables.
    """

    format_name: ClassVar[str] = "fits"
    extensions: ClassVar[list[str]] = [".fits", ".fit", ".fts"]

    def can_handle(self, file_path: Path) -> bool:
        """
        Check if file is a valid FITS file.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if file has FITS extension and exists
        """
        return file_path.suffix.lower() in self.extensions and file_path.exists()

    def extract_metadata(self, file_path: Path) -> dict[str, Any]:
        """
        Extract metadata from FITS file headers.

        Parameters
        ----------
        file_path : Path
            Path to FITS file

        Returns
        -------
        dict[str, Any]
            Metadata from primary header and HDU information
        """
        try:
            from astropy.io import fits

            with fits.open(file_path) as hdul:
                primary_header = dict(hdul[0].header)
                metadata = {
                    "format": "fits",
                    "n_hdus": len(hdul),
                    "hdu_info": [
                        {
                            "index": i,
                            "name": hdu.name,
                            "type": type(hdu).__name__,
                        }
                        for i, hdu in enumerate(hdul)
                    ],
                    "primary_header": primary_header,
                }
                return metadata

        except (ImportError, OSError):
            return {
                "format": "fits",
                "error": "Cannot read FITS file",
            }

    def validate(self, file_path: Path) -> bool:
        """
        Validate FITS file integrity.

        Parameters
        ----------
        file_path : Path
            Path to file

        Returns
        -------
        bool
            True if file can be opened and has valid structure
        """
        try:
            from astropy.io import fits

            with fits.open(file_path) as hdul:
                return len(hdul) > 0

        except (ImportError, OSError):
            return False
