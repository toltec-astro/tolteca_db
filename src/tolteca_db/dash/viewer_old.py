"""Database browser viewer component using Template pattern."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objs as go
from dash import Input, Output, dcc, html
from dash_component_template import Template
import dash_mantine_components as dmc
from loguru import logger
from sqlalchemy import create_engine, inspect, select, func, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from tolteca_db.models.orm import (
    Base,
    DataProd,
    DataProdSource,
    DataProdType,
    DataKind,
    Location,
    Flag,
    ReductionTask,
)


class DBBrowserViewer(Template):
    """Database browser viewer component using Template pattern.
    
    Displays database schema, table contents, and ingestion summary
    following tolteca_web component patterns with modern DMC UI.
    
    Parameters
    ----------
    db_url : str
        Database connection URL
    """
    
    def __init__(self, db_url: str):
        super().__init__()
        
        self.db_url = db_url
        # DuckDB only supports single writer - use StaticPool to ensure
        # all threads share the same connection and avoid WAL corruption
        self.engine = create_engine(
            db_url,
            poolclass=StaticPool,
            connect_args={"read_only": False}
        )
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        # Force checkpoint to flush WAL and prevent corruption
        try:
            with self.engine.begin() as conn:
                conn.execute(text("CHECKPOINT"))
        except Exception:
            pass  # Checkpoint might not be needed in all cases
        
        self.inspector = inspect(self.engine)
        
        # Create UI components
        self._build_layout()
    
    def _build_layout(self):
        """Build the component tree."""
        # Main container
        container = self.child[dmc.Container](fluid=True, p="lg")
        
        # Header
        header = container.child[dmc.Title]("TolTEC Data Product Database", order=1, mb="md")
        
        # Tabs
        self.tabs = container.child[dmc.Tabs](
            value="summary",
            variant="pills",
            mb="lg",
        )
        
        # Tab list
        tab_list = self.tabs.child[dmc.TabsList]()
        tab_list.child[dmc.TabsTab]("Summary", value="summary")
        tab_list.child[dmc.TabsTab]("Schema", value="schema")
        tab_list.child[dmc.TabsTab]("Data Products", value="data_prod")
        tab_list.child[dmc.TabsTab]("Sources", value="sources")
        tab_list.child[dmc.TabsTab]("Locations", value="locations")
        tab_list.child[dmc.TabsTab]("Tasks", value="tasks")
        
        # Tab content container
        self.tab_content = container.child[html.Div]()
    
    def setup_callbacks(self, app):
        """Setup integrated callbacks."""
        
        @app.callback(
            Output(self.tab_content(), "children"),
            Input(self.tabs(), "value"),
        )
        def render_tab_content(active_tab):
            if active_tab == "summary":
                return self._render_summary()
            elif active_tab == "schema":
                return self._render_schema()
            elif active_tab == "data_prod":
                return self._render_table_content("data_prod")
            elif active_tab == "sources":
                return self._render_table_content("data_prod_source")
            elif active_tab == "locations":
                return self._render_table_content("location")
            elif active_tab == "tasks":
                return self._render_table_content("reduction_task")
            return dmc.Text("Select a tab", c="dimmed")
    
    def _render_summary(self):
        """Render ingestion summary."""
        with Session(self.engine) as session:
            # Get total counts first to check if database is empty
            total_products = session.query(func.count(DataProd.pk)).scalar() or 0
            total_sources = session.query(func.count(DataProdSource.source_uri)).scalar() or 0
            total_locations = session.query(func.count(Location.pk)).scalar() or 0
            total_tasks = session.query(func.count(ReductionTask.pk)).scalar() or 0
            
            # If database is empty, show helpful message
            if total_products == 0 and total_sources == 0:
                return dbc.Alert(
                    [
                        html.H4("Database is Empty", className="alert-heading"),
                        html.P(
                            "No data has been ingested yet. The database schema has been created, "
                            "but there are no data products or sources."
                        ),
                        html.Hr(),
                        html.P(
                            "To ingest data, use the tolteca_db CLI commands or run the ingestion pipeline.",
                            className="mb-0",
                        ),
                    ],
                    color="info",
                )
            
            # Count data products by type
            data_prod_counts = (
                session.query(
                    DataProdType.label,
                    func.count(DataProd.pk).label("count")
                )
                .join(DataProd.data_prod_type)
                .group_by(DataProdType.label)
                .all()
            )
            
            # Count sources by location
            source_counts = (
                session.query(
                    Location.label,
                    func.count(DataProdSource.source_uri).label("count")
                )
                .join(DataProdSource.location)
                .group_by(Location.label)
                .all()
            )
            
            # Get observation range for raw data
            obs_range = (
                session.execute(
                    select(
                        func.min(DataProd.meta['obsnum'].as_integer()).label('min_obsnum'),
                        func.max(DataProd.meta['obsnum'].as_integer()).label('max_obsnum')
                    )
                    .join(DataProd.data_prod_type)
                    .where(DataProdType.label == 'dp_raw_obs')
                )
                .first()
            )
        
        # Create summary cards
        summary_cards = dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4(total_products, className="text-primary"),
                                html.P("Total Data Products"),
                            ]
                        ),
                        className="text-center",
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4(total_sources, className="text-success"),
                                html.P("Total Sources"),
                            ]
                        ),
                        className="text-center",
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4(total_locations, className="text-info"),
                                html.P("Storage Locations"),
                            ]
                        ),
                        className="text-center",
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4(total_tasks, className="text-warning"),
                                html.P("Reduction Tasks"),
                            ]
                        ),
                        className="text-center",
                    ),
                    width=3,
                ),
            ],
            className="mb-4",
        )
        
        # Create data product type chart
        if data_prod_counts:
            prod_labels = [row[0] for row in data_prod_counts]
            prod_values = [row[1] for row in data_prod_counts]
            
            prod_chart = dcc.Graph(
                figure=go.Figure(
                    data=[
                        go.Bar(
                            x=prod_labels,
                            y=prod_values,
                            marker_color='steelblue',
                        )
                    ],
                    layout=go.Layout(
                        title="Data Products by Type",
                        xaxis_title="Product Type",
                        yaxis_title="Count",
                        height=400,
                    ),
                )
            )
        else:
            prod_chart = html.Div("No data products found")
        
        # Create source location chart
        if source_counts:
            loc_labels = [row[0] for row in source_counts]
            loc_values = [row[1] for row in source_counts]
            
            loc_chart = dcc.Graph(
                figure=go.Figure(
                    data=[
                        go.Pie(
                            labels=loc_labels,
                            values=loc_values,
                            hole=0.3,
                        )
                    ],
                    layout=go.Layout(
                        title="Sources by Location",
                        height=400,
                    ),
                )
            )
        else:
            loc_chart = html.Div("No sources found")
        
        # Observation range info
        obs_range_info = dbc.Alert(
            [
                html.H5("Raw Observation Range", className="alert-heading"),
                html.P(
                    f"Obsnum: {obs_range[0] or 'N/A'} - {obs_range[1] or 'N/A'}"
                    if obs_range else "No raw observations ingested"
                ),
            ],
            color="info",
            className="mb-4",
        )
        
        return html.Div(
            [
                summary_cards,
                obs_range_info,
                dbc.Row(
                    [
                        dbc.Col(prod_chart, width=6),
                        dbc.Col(loc_chart, width=6),
                    ]
                ),
            ]
        )
    
    def _render_schema(self):
        """Render database schema information."""
        tables = self.inspector.get_table_names()
        
        schema_info = []
        for table_name in sorted(tables):
            columns = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            fk_constraints = self.inspector.get_foreign_keys(table_name)
            
            # Build column table
            col_data = []
            for col in columns:
                col_info = {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": "Yes" if col["nullable"] else "No",
                    "default": str(col.get("default", "")),
                    "primary_key": "✓" if col["name"] in pk_constraint.get("constrained_columns", []) else "",
                }
                col_data.append(col_info)
            
            col_df = pd.DataFrame(col_data)
            
            # Build FK info
            fk_info = []
            for fk in fk_constraints:
                fk_text = f"{', '.join(fk['constrained_columns'])} → {fk['referred_table']}.{', '.join(fk['referred_columns'])}"
                fk_info.append(html.Li(fk_text))
            
            schema_info.append(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H5(
                                [
                                    html.I(className="fas fa-table me-2"),
                                    table_name,
                                ],
                                className="mb-3",
                            ),
                            html.H6("Columns:", className="mt-3"),
                            dash_table.DataTable(
                                data=col_df.to_dict("records"),
                                columns=[{"name": i, "id": i} for i in col_df.columns],
                                style_table={"overflowX": "auto"},
                                style_cell={
                                    "textAlign": "left",
                                    "padding": "10px",
                                },
                                style_header={
                                    "backgroundColor": "rgb(230, 230, 230)",
                                    "fontWeight": "bold",
                                },
                            ),
                            html.H6("Foreign Keys:", className="mt-3") if fk_info else None,
                            html.Ul(fk_info) if fk_info else None,
                        ]
                    ),
                    className="mb-3",
                )
            )
        
        return html.Div(schema_info)
    
    def _render_table_content(self, table_name: str):
        """Render table content."""
        try:
            with Session(self.engine) as session:
                # Get table data (limit to 1000 rows)
                query = text(f"SELECT * FROM {table_name} LIMIT 1000")
                df = pd.read_sql(query, session.connection())
                
                if df.empty:
                    return dbc.Alert(
                        f"No data in table '{table_name}'",
                        color="warning",
                    )
                
                # Convert to display format (truncate long strings)
                display_df = df.copy()
                for col in display_df.columns:
                    if display_df[col].dtype == object:
                        display_df[col] = display_df[col].astype(str).str[:100]
                
                return html.Div(
                    [
                        dbc.Alert(
                            f"Showing {len(df)} rows from '{table_name}' (limited to 1000)",
                            color="info",
                            className="mb-3",
                        ),
                        dash_table.DataTable(
                            data=display_df.to_dict("records"),
                            columns=[{"name": i, "id": i} for i in display_df.columns],
                            page_size=20,
                            filter_action="native",
                            sort_action="native",
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "textAlign": "left",
                                "padding": "10px",
                                "minWidth": "100px",
                                "maxWidth": "300px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            style_header={
                                "backgroundColor": "rgb(230, 230, 230)",
                                "fontWeight": "bold",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"row_index": "odd"},
                                    "backgroundColor": "rgb(248, 248, 248)",
                                }
                            ],
                        ),
                    ]
                )
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {e}")
            return dbc.Alert(
                [
                    html.H5("Error", className="alert-heading"),
                    html.P(f"Failed to load table '{table_name}': {str(e)}"),
                ],
                color="danger",
            )
