"""Dash application factory for tolteca_db browser."""

from __future__ import annotations

from typing import TYPE_CHECKING

import dash

if TYPE_CHECKING:
    from flask import Flask

from tolteca_db.dash.viewer import DBBrowserViewer


def create_app(db_url: str = "duckdb:///tolteca.duckdb", debug: bool = False) -> Flask:
    """Create Dash application for database browsing.
    
    Parameters
    ----------
    db_url : str, optional
        Database URL to connect to, by default "duckdb:///tolteca.duckdb"
    debug : bool, optional
        Enable debug mode, by default False
    
    Returns
    -------
    Flask
        Flask server instance
    """
    # Create Dash app with Mantine components
    app = dash.Dash(
        __name__,
        suppress_callback_exceptions=True,
        title="TolTEC Database Browser",
    )
    
    # Create viewer component (Template pattern)
    viewer = DBBrowserViewer(db_url=db_url)
    
    # Set up layout using Template's layout() method
    app.layout = viewer.layout()
    
    # Register callbacks (Template pattern)
    viewer.register_callbacks(app)
    
    # Enable dev tools if debug
    if debug:
        app.enable_dev_tools(debug=True)
    
    return app.server


if __name__ == "__main__":
    server = create_app(debug=True)
    server.run(debug=True, port=8050)
