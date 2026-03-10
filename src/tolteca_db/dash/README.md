# TolTEC Database Browser - Dash Web Interface

A Dash-based web application for browsing and visualizing the tolteca_db database.

## Features

- **Summary Dashboard**: Overview of ingested data with statistics and charts
  - Total counts for data products, sources, locations, and tasks
  - Data products by type (bar chart)
  - Sources by location (pie chart)
  - Observation range for raw data

- **Schema Viewer**: Database schema browser
  - Table structure with columns, types, and constraints
  - Primary keys and foreign keys
  - Interactive table cards

- **Table Browser**: Content viewer for all tables
  - Paginated data display (max 1000 rows per query)
  - Native filtering and sorting
  - Responsive table layout

## Installation

Install the optional dash dependencies:

```bash
cd tolteca_db
uv pip install 'dash>=2.14.0' 'dash-bootstrap-components>=1.5.0' 'plotly>=5.18.0'
```

## Usage

### Start the development server

```bash
# Default database (duckdb:///tolteca.duckdb)
python -m tolteca_db dash serve

# Custom database URL
python -m tolteca_db dash serve --db-url duckdb:///path/to/db.duckdb

# Custom host and port
python -m tolteca_db dash serve --host 0.0.0.0 --port 9000

# Enable debug mode (with live reload)
python -m tolteca_db dash serve --debug
```

The app will be available at http://127.0.0.1:8050 by default.

### Usage with existing database

If you have already ingested data using the `tolteca-db ingest` commands:

```bash
# Use the default database location
python -m tolteca_db dash serve

# Or specify the database explicitly
python -m tolteca_db dash serve --db-url duckdb:///tolteca.duckdb
```

## Architecture

The Dash app follows the tolteca_web component template pattern:

- `app.py` - Application factory with layout setup
- `viewer.py` - Main viewer component with tabs and callbacks
- `dash_commands.py` - CLI integration

### Component Structure

```
DBBrowserViewer
├── Summary Tab (statistics and charts)
├── Schema Tab (database structure)
├── Data Products Tab (table content)
├── Sources Tab (table content)
├── Locations Tab (table content)
└── Tasks Tab (table content)
```

## Development

The viewer uses:
- **Dash Bootstrap Components** for UI layout
- **Plotly** for interactive charts
- **SQLAlchemy** for database queries
- **Pandas** for data manipulation

### Adding New Tabs

To add a new tab, modify `viewer.py`:

1. Add tab to the `Tabs` component in `layout` property
2. Add case to `render_tab_content` callback
3. Implement render method (e.g., `_render_new_tab()`)

### Custom Queries

The viewer uses SQLAlchemy ORM for type-safe queries. For custom visualizations, add methods to the `DBBrowserViewer` class.

## Screenshots

### Summary Dashboard
Shows key metrics and charts for ingested data:
- Total counts (products, sources, locations, tasks)
- Bar chart of products by type
- Pie chart of sources by location
- Observation number range

### Schema Browser
Interactive database schema explorer:
- Table list with column details
- Data types and constraints
- Primary and foreign key relationships

### Table Browsers
Paginated table viewers with:
- Sortable columns
- Filterable data
- Responsive layout
- Truncated long strings

## Notes

- Maximum 1000 rows per table query (for performance)
- Long strings are truncated to 100 characters in table display
- Debug mode enables live code reloading and detailed error messages
- The app reads data using SQLAlchemy ORM with proper relationship loading
