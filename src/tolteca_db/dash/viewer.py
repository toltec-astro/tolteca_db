"""Database browser viewer component using Template pattern."""

from __future__ import annotations

import json
import pandas as pd
import plotly.graph_objs as go
from dash import Input, Output, dcc, html
from dash_component_template import Template
import dash_mantine_components as dmc
import dash_extensions as de
import dash_cytoscape as cyto
from loguru import logger
from sqlalchemy import create_engine, inspect, select, func, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

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
import json


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
        # Use NullPool to avoid connection sharing issues with Flask's debug mode
        # NullPool creates a new connection for each request and properly disposes it
        # This prevents "Commands out of sync" errors when Flask reloader forks the process
        self.engine = create_engine(
            db_url,
            poolclass=NullPool,
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
        # Main container with Mantine theme provider
        container = self.child[dmc.MantineProvider](
            theme={"colorScheme": "light"},
            children=[],
        )
        
        main_container = container.child[dmc.Container](fluid=True, p="lg")
        
        # Header
        header = main_container.child[dmc.Title]("TolTEC Data Product Database", order=1, mb="md")
        
        # Tabs
        self.tabs = main_container.child[dmc.Tabs](
            value="summary",
            variant="pills",
            mb="lg",
        )
        
        # Tab list
        tab_list = self.tabs.child[dmc.TabsList]()
        tab_list.child[dmc.TabsTab]("Summary", value="summary")
        tab_list.child[dmc.TabsTab]("Schema", value="schema")
        tab_list.child[dmc.TabsTab]("Registry", value="registry")
        tab_list.child[dmc.TabsTab]("Associations", value="associations")
        tab_list.child[dmc.TabsTab]("Data Products", value="data_prod")
        tab_list.child[dmc.TabsTab]("Sources", value="sources")
        tab_list.child[dmc.TabsTab]("Locations", value="locations")
        tab_list.child[dmc.TabsTab]("Tasks", value="tasks")
        
        # Tab content container
        self.tab_content = main_container.child[html.Div]()
    
    def _dict_to_tree(self, data, parent_key=""):
        """Convert nested dict to tree structure for dmc.Tree."""
        if not isinstance(data, dict):
            return None
        
        tree_data = []
        for key, value in data.items():
            node_value = f"{parent_key}.{key}" if parent_key else key
            
            if isinstance(value, dict) and value:
                # Nested dict - create expandable node
                children = self._dict_to_tree(value, node_value)
                tree_data.append({
                    "value": node_value,
                    "label": f"{key}: {{...}}",
                    "children": children if children else [],
                })
            elif isinstance(value, (list, tuple)) and len(value) > 0:
                # List - show length and first few items
                tree_data.append({
                    "value": node_value,
                    "label": f"{key}: [{len(value)} items]",
                })
            else:
                # Leaf node - show value
                val_str = str(value)[:100]
                tree_data.append({
                    "value": node_value,
                    "label": f"{key}: {val_str}",
                })
        
        return tree_data
    
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
            elif active_tab == "registry":
                return self._render_registry()
            elif active_tab == "associations":
                return self._render_associations()
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
        """Render ingestion summary with DMC components."""
        with Session(self.engine) as session:
            # Get total counts
            total_products = session.query(func.count(DataProd.pk)).scalar() or 0
            total_sources = session.query(func.count(DataProdSource.source_uri)).scalar() or 0
            total_locations = session.query(func.count(Location.pk)).scalar() or 0
            total_tasks = session.query(func.count(ReductionTask.pk)).scalar() or 0
            
            # If database is empty
            if total_products == 0 and total_sources == 0:
                return dmc.Alert(
                    title="Database is Empty",
                    children="No data has been ingested yet. To ingest data, use the tolteca_db CLI commands.",
                    color="blue",
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
            
            # Count associations by type
            assoc_counts = session.execute(
                text("""
                    SELECT 
                        dpat.label,
                        COUNT(*) as count
                    FROM data_prod_assoc dpa
                    JOIN data_prod_assoc_type dpat ON dpa.data_prod_assoc_type_fk = dpat.pk
                    GROUP BY dpat.label
                    ORDER BY count DESC
                """)
            ).fetchall()
            
            # Get observation range
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
        
        # Summary cards
        summary_cards = dmc.SimpleGrid(
            cols=4,
            spacing="lg",
            mb="lg",
            children=[
                dmc.Card(
                    children=[
                        dmc.Text(str(total_products), size="xl", fw=700, c="blue"),
                        dmc.Text("Total Data Products", size="sm", c="dimmed"),
                    ],
                    withBorder=True,
                    p="md",
                    radius="md",
                ),
                dmc.Card(
                    children=[
                        dmc.Text(str(total_sources), size="xl", fw=700, c="green"),
                        dmc.Text("Total Sources", size="sm", c="dimmed"),
                    ],
                    withBorder=True,
                    p="md",
                    radius="md",
                ),
                dmc.Card(
                    children=[
                        dmc.Text(str(total_locations), size="xl", fw=700, c="cyan"),
                        dmc.Text("Storage Locations", size="sm", c="dimmed"),
                    ],
                    withBorder=True,
                    p="md",
                    radius="md",
                ),
                dmc.Card(
                    children=[
                        dmc.Text(str(total_tasks), size="xl", fw=700, c="orange"),
                        dmc.Text("Reduction Tasks", size="sm", c="dimmed"),
                    ],
                    withBorder=True,
                    p="md",
                    radius="md",
                ),
            ],
        )
        
        # Charts
        if data_prod_counts:
            prod_chart = dcc.Graph(
                figure=go.Figure(
                    data=[go.Bar(
                        x=[row[0] for row in data_prod_counts],
                        y=[row[1] for row in data_prod_counts],
                        marker_color='steelblue',
                    )],
                    layout=go.Layout(
                        title="Data Products by Type",
                        xaxis_title="Product Type",
                        yaxis_title="Count",
                        height=400,
                        template="plotly_white",
                    ),
                )
            )
        else:
            prod_chart = dmc.Text("No data products found", c="dimmed")
        
        if source_counts:
            loc_chart = dcc.Graph(
                figure=go.Figure(
                    data=[go.Pie(
                        labels=[row[0] for row in source_counts],
                        values=[row[1] for row in source_counts],
                        hole=0.3,
                    )],
                    layout=go.Layout(
                        title="Sources by Location",
                        height=400,
                        template="plotly_white",
                    ),
                )
            )
        else:
            loc_chart = dmc.Text("No sources found", c="dimmed")
        
        # Associations chart
        if assoc_counts:
            assoc_chart = dcc.Graph(
                figure=go.Figure(
                    data=[go.Bar(
                        x=[row[0] for row in assoc_counts],
                        y=[row[1] for row in assoc_counts],
                        marker_color='lightcoral',
                    )],
                    layout=go.Layout(
                        title="Associations by Type",
                        xaxis_title="Association Type",
                        yaxis_title="Count",
                        height=400,
                        template="plotly_white",
                    ),
                )
            )
        else:
            assoc_chart = dmc.Text("No associations found", c="dimmed")
        
        # Observation range
        obs_range_info = dmc.Alert(
            title="Raw Observation Range",
            children=f"Obsnum: {obs_range[0] or 'N/A'} - {obs_range[1] or 'N/A'}" if obs_range else "No raw observations",
            color="blue",
            mb="lg",
        )
        
        return html.Div([
            summary_cards,
            obs_range_info,
            dmc.SimpleGrid(cols=3, spacing="lg", children=[prod_chart, loc_chart, assoc_chart]),
        ])
    
    def _generate_erd_mermaid(self) -> str:
        """Generate Mermaid ERD diagram code for database schema."""
        tables = self.inspector.get_table_names()
        
        # Start Mermaid ERD diagram
        mermaid_lines = ["erDiagram"]
        
        # Track processed relationships to avoid duplicates
        processed_rels = set()
        
        for table_name in sorted(tables):
            columns = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            fk_constraints = self.inspector.get_foreign_keys(table_name)
            
            pk_cols = set(pk_constraint.get('constrained_columns', []))
            
            # Sanitize table name for Mermaid (replace special chars)
            safe_table = table_name.replace('-', '_').replace('.', '_')
            
            # Add table with attributes
            mermaid_lines.append(f"    {safe_table} {{")
            
            for col in columns:
                col_name = col['name'].replace('-', '_').replace('.', '_')
                col_type = str(col['type']).upper()
                
                # Sanitize type (remove parentheses and special chars)
                col_type = col_type.split('(')[0].replace(' ', '_')
                
                # Avoid Mermaid reserved keywords by prefixing with underscore if needed
                # Known reserved words: pk, fk, uk
                if col_name.lower() in ('pk', 'fk', 'uk'):
                    col_name = f'_{col_name}'
                
                # Mermaid ERD format: type attribute_name key_indicator
                # PK goes after the attribute name, not in quotes
                key_indicator = ""
                if col['name'] in pk_cols:  # Use original name for PK check
                    key_indicator = " PK"
                
                mermaid_lines.append(f'        {col_type} {col_name}{key_indicator}')
            
            mermaid_lines.append("    }")
            
            # Add relationships
            for fk in fk_constraints:
                referred_table = fk['referred_table'].replace('-', '_').replace('.', '_')
                
                # Create unique relationship key
                rel_key = f"{safe_table}-{referred_table}"
                rev_key = f"{referred_table}-{safe_table}"
                
                # Only add if not already processed (avoid duplicates)
                if rel_key not in processed_rels and rev_key not in processed_rels:
                    # Many-to-one relationship (foreign key)
                    mermaid_lines.append(f"    {safe_table} }}o--|| {referred_table} : has")
                    processed_rels.add(rel_key)
        
        return "\n".join(mermaid_lines)
    
    def _render_registry(self):
        """Render registry/lookup tables content."""
        registry_tables = [
            ("data_prod_type", "Data Product Types"),
            ("data_kind", "Data Kinds"),
            ("data_prod_assoc_type", "Association Types"),
            ("flag", "Quality Flags"),
        ]
        
        cards = []
        
        for table_name, title in registry_tables:
            # Query table content
            with Session(self.engine) as session:
                result = session.execute(text(f"SELECT * FROM {table_name}"))
                columns = result.keys()
                rows = result.fetchall()
            
            if not rows:
                cards.append(
                    dmc.Card([
                        dmc.Title(title, order=4, mb="md"),
                        dmc.Text("No entries", c="dimmed"),
                    ], withBorder=True, p="md", mb="lg")
                )
                continue
            
            # Create table rows
            table_rows = []
            for row in rows:
                cells = []
                for col_name, value in zip(columns, row):
                    # Format value
                    if value is None:
                        val_str = "—"
                    elif isinstance(value, (dict, list)):
                        val_str = json.dumps(value, indent=2)
                    else:
                        val_str = str(value)
                    
                    cells.append(html.Td(val_str))
                table_rows.append(html.Tr(cells))
            
            # Create table
            table = dmc.Table([
                html.Thead(
                    html.Tr([html.Th(col) for col in columns])
                ),
                html.Tbody(table_rows)
            ], striped=True, highlightOnHover=True, withTableBorder=True, withColumnBorders=True)
            
            cards.append(
                dmc.Card([
                    dmc.Title(title, order=4, mb="md"),
                    dmc.Text(
                        f"{len(rows)} entries",
                        size="sm",
                        c="dimmed",
                        mb="md"
                    ),
                    table,
                ], withBorder=True, p="md", mb="lg")
            )
        
        return dmc.Stack(cards, gap="md")
    
    def _render_associations(self):
        """Render data product associations."""
        with Session(self.engine) as session:
            # Query associations with related data
            query = text("""
                SELECT 
                    dpa.pk,
                    dpa.src_data_prod_fk,
                    dpa.dst_data_prod_fk,
                    dpa.data_prod_assoc_type_fk,
                    dpa.context,
                    dpa.created_at,
                    dpat.label as assoc_type_label,
                    json_extract(parent_dp.meta, '$.name') as parent_name,
                    parent_dpt.label as parent_type,
                    json_extract(child_dp.meta, '$.name') as child_name,
                    child_dpt.label as child_type
                FROM data_prod_assoc dpa
                JOIN data_prod_assoc_type dpat ON dpa.data_prod_assoc_type_fk = dpat.pk
                JOIN data_prod parent_dp ON dpa.src_data_prod_fk = parent_dp.pk
                JOIN data_prod_type parent_dpt ON parent_dp.data_prod_type_fk = parent_dpt.pk
                JOIN data_prod child_dp ON dpa.dst_data_prod_fk = child_dp.pk
                JOIN data_prod_type child_dpt ON child_dp.data_prod_type_fk = child_dpt.pk
                ORDER BY dpa.created_at DESC
                LIMIT 500
            """)
            result = session.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            
            if not rows:
                return dmc.Alert(
                    title="No Associations",
                    children="No data product associations found. Run association generation to create them.",
                    color="blue",
                )
            
            # Build Cytoscape graph elements
            nodes = {}
            edges = []
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                src_id = f"dp_{row_dict['src_data_prod_fk']}"
                dst_id = f"dp_{row_dict['dst_data_prod_fk']}"
                
                # Add source node
                if src_id not in nodes:
                    nodes[src_id] = {
                        'data': {
                            'id': src_id,
                            'label': row_dict['parent_name'] or f"Product #{row_dict['src_data_prod_fk']}",
                            'type': row_dict['parent_type'],
                        }
                    }
                
                # Add destination node
                if dst_id not in nodes:
                    nodes[dst_id] = {
                        'data': {
                            'id': dst_id,
                            'label': row_dict['child_name'] or f"Product #{row_dict['dst_data_prod_fk']}",
                            'type': row_dict['child_type'],
                        }
                    }
                
                # Add edge
                edges.append({
                    'data': {
                        'source': src_id,
                        'target': dst_id,
                        'label': row_dict['assoc_type_label'],
                        'context': row_dict['context'],
                    }
                })
            
            # Combine nodes and edges
            elements = list(nodes.values()) + edges
            
            # Create Cytoscape graph
            cytoscape_graph = cyto.Cytoscape(
                id='association-graph',
                elements=elements,
                layout={
                    'name': 'cose',
                    'animate': True,
                    'animationDuration': 500,
                    'nodeRepulsion': 8000,
                    'idealEdgeLength': 100,
                },
                style={'width': '100%', 'height': '600px'},
                stylesheet=[
                    {
                        'selector': 'node',
                        'style': {
                            'label': 'data(label)',
                            'background-color': '#0066cc',
                            'color': '#fff',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '10px',
                            'width': '60px',
                            'height': '60px',
                            'text-wrap': 'wrap',
                            'text-max-width': '80px',
                        }
                    },
                    {
                        'selector': 'edge',
                        'style': {
                            'width': 2,
                            'line-color': '#999',
                            'target-arrow-color': '#999',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'label': 'data(label)',
                            'font-size': '8px',
                            'text-rotation': 'autorotate',
                        }
                    }
                ]
            )
            
            # Build table rows
            table_rows = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                cells = [
                    html.Td(str(row_dict['pk'])),
                    html.Td(row_dict['assoc_type_label'], style={"fontWeight": "500"}),
                    html.Td([
                        html.Div(row_dict['parent_name'] or f"Product #{row_dict['src_data_prod_fk']}", style={"fontWeight": "500"}),
                        html.Div(row_dict['parent_type'], style={"fontSize": "0.85em", "color": "#868e96"}),
                    ]),
                    html.Td([
                        html.Div(row_dict['child_name'] or f"Product #{row_dict['dst_data_prod_fk']}", style={"fontWeight": "500"}),
                        html.Div(row_dict['child_type'], style={"fontSize": "0.85em", "color": "#868e96"}),
                    ]),
                    html.Td(row_dict['context'] or "—", style={"fontSize": "0.9em"}),
                    html.Td(str(row_dict['created_at'])[:19] if row_dict['created_at'] else "—", style={"fontSize": "0.85em", "color": "#868e96"}),
                ]
                table_rows.append(html.Tr(cells))
            
            # Create table
            table = dmc.Table([
                html.Thead(
                    html.Tr([
                        html.Th("ID"),
                        html.Th("Association Type"),
                        html.Th("Source Product"),
                        html.Th("Destination Product"),
                        html.Th("Context"),
                        html.Th("Created"),
                    ])
                ),
                html.Tbody(table_rows)
            ], striped=True, highlightOnHover=True, withTableBorder=True, withColumnBorders=True)
            
            # Get summary statistics
            stats_query = text("""
                SELECT 
                    dpat.label,
                    COUNT(*) as count
                FROM data_prod_assoc dpa
                JOIN data_prod_assoc_type dpat ON dpa.data_prod_assoc_type_fk = dpat.pk
                GROUP BY dpat.label
                ORDER BY count DESC
            """)
            stats_result = session.execute(stats_query)
            stats_rows = stats_result.fetchall()
            
            # Build stats badges
            stats_badges = []
            for label, count in stats_rows:
                stats_badges.append(
                    dmc.Badge(f"{label}: {count}", variant="light", size="lg", mr="sm")
                )
            
            return dmc.Stack([
                dmc.Card([
                    dmc.Title("Association Graph", order=4, mb="md"),
                    dmc.Text(
                        f"Interactive network graph showing {len(nodes)} data products and {len(edges)} associations",
                        size="sm",
                        c="dimmed",
                        mb="md"
                    ),
                    cytoscape_graph,
                ], withBorder=True, p="md"),
                dmc.Card([
                    dmc.Title("Data Product Associations", order=4, mb="md"),
                    dmc.Text(
                        f"{len(rows)} associations (showing up to 500 most recent)",
                        size="sm",
                        c="dimmed",
                        mb="md"
                    ),
                    dmc.Group(stats_badges, mb="md") if stats_badges else None,
                    table,
                ], withBorder=True, p="md"),
            ], gap="md")
    
    def _render_schema(self):
        """Render database schema with interactive accordion view."""
        tables = self.inspector.get_table_names()
        
        # Create accordion items for each table
        accordion_items = []
        for table_name in sorted(tables):
            columns = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            fk_constraints = self.inspector.get_foreign_keys(table_name)
            
            # Get PK and FK column names
            pk_cols = set(pk_constraint.get('constrained_columns', []))
            fk_cols = set()
            fk_info = {}
            for fk in fk_constraints:
                for col in fk['constrained_columns']:
                    fk_cols.add(col)
                    fk_info[col] = f"{fk['referred_table']}.{fk['referred_columns'][0]}"
            
            # Build column table data
            column_data = []
            for col in columns:
                col_name = col['name']
                col_type = str(col['type'])
                nullable = "NULL" if col.get('nullable', True) else "NOT NULL"
                
                keys = []
                if col_name in pk_cols:
                    keys.append("🔑 PK")
                if col_name in fk_cols:
                    keys.append(f"🔗 FK → {fk_info[col_name]}")
                
                key_str = " ".join(keys) if keys else ""
                
                column_data.append({
                    "Column": col_name,
                    "Type": col_type,
                    "Nullable": nullable,
                    "Keys": key_str,
                })
            
            # Get row count
            try:
                with Session(self.engine) as session:
                    row_count = session.scalar(
                        select(func.count()).select_from(text(table_name))
                    )
            except Exception:
                row_count = "N/A"
            
            # Create accordion item
            accordion_item = dmc.AccordionItem(
                [
                    dmc.AccordionControl(
                        dmc.Group([
                            dmc.Text(table_name, fw=600, size="sm"),
                            dmc.Badge(f"{row_count} rows", size="sm", variant="light"),
                            dmc.Badge(f"{len(columns)} cols", size="sm", variant="outline"),
                        ], gap="sm")
                    ),
                    dmc.AccordionPanel(
                        dmc.Table([
                            html.Thead(html.Tr([
                                html.Th("Column"),
                                html.Th("Type"),
                                html.Th("Nullable"),
                                html.Th("Keys/References"),
                            ])),
                            html.Tbody([
                                html.Tr([
                                    html.Td(row["Column"], style={"fontFamily": "monospace"}),
                                    html.Td(row["Type"], style={"color": "#228be6"}),
                                    html.Td(row["Nullable"], style={"fontSize": "0.85em"}),
                                    html.Td(row["Keys"], style={"fontSize": "0.85em"}),
                                ]) for row in column_data
                            ]),
                        ], striped=True, highlightOnHover=True, withColumnBorders=True, fz="sm")
                    ),
                ],
                value=table_name,
            )
            accordion_items.append(accordion_item)
        
        # Generate Mermaid ERD diagram
        mermaid_code = self._generate_erd_mermaid()
        
        return dmc.Stack([
            dmc.Card([
                dmc.Title("Database Schema - ERD Diagram", order=4, mb="md"),
            dmc.Text(
                "Entity Relationship Diagram showing table relationships",
                size="sm",
                c="dimmed",
                mb="md"
            ),
            de.Mermaid(
                id="schema-erd",
                chart=mermaid_code,
            ),
        ], withBorder=True, p="md", mb="lg"),            dmc.Card([
                dmc.Title("Database Schema - Table Browser", order=4, mb="md"),
                dmc.Text(
                    f"Interactive schema browser - {len(tables)} tables",
                    size="sm",
                    c="dimmed",
                    mb="md"
                ),
                dmc.Accordion(
                    children=accordion_items,
                    variant="separated",
                    chevronPosition="left",
                ),
            ], withBorder=True, p="md", mb="lg"),
        ], gap="md")
    
    def _render_table_content(self, table_name: str):
        """Render table content with DMC components."""
        try:
            with Session(self.engine) as session:
                query = text(f"SELECT * FROM {table_name} LIMIT 1000")
                df = pd.read_sql(query, session.connection())
                
                if df.empty:
                    return dmc.Alert(f"No data in table '{table_name}'", color="yellow")
                
                # Check if table has meta column
                has_meta = 'meta' in df.columns
                
                # Build rows with special handling for meta column
                rows_data = []
                for idx, row in df.head(100).iterrows():
                    row_cells = []
                    for col in df.columns:
                        val = row[col]
                        
                        if col == 'meta' and has_meta:
                            # Render meta as expandable JSON tree
                            if val and isinstance(val, (dict, str)):
                                try:
                                    meta_dict = val if isinstance(val, dict) else json.loads(val)
                                    tree_data = self._dict_to_tree(meta_dict)
                                    if tree_data:
                                        cell_content = dmc.Accordion([
                                            dmc.AccordionItem([
                                                dmc.AccordionControl("View Metadata"),
                                                dmc.AccordionPanel(
                                                    dmc.Tree(
                                                        data=tree_data,
                                                        selectOnClick=False,
                                                    )
                                                ),
                                            ], value=f"meta-{idx}")
                                        ], chevronPosition="left")
                                    else:
                                        cell_content = dmc.Text(str(val)[:50], size="sm", c="dimmed")
                                except (json.JSONDecodeError, TypeError):
                                    cell_content = dmc.Text(str(val)[:50], size="sm", c="dimmed")
                            else:
                                cell_content = dmc.Text("null", size="sm", c="dimmed")
                            row_cells.append(dmc.TableTd(cell_content))
                        else:
                            # Regular column - truncate long text
                            row_cells.append(dmc.TableTd(str(val)[:100]))
                    
                    rows_data.append(dmc.TableTr(row_cells))
                
                # Build table header
                header_row = dmc.TableThead(
                    dmc.TableTr([
                        dmc.TableTh(col) for col in df.columns
                    ])
                )
                
                table = dmc.Table(
                    [header_row, dmc.TableTbody(rows_data)],
                    striped=True,
                    highlightOnHover=True,
                    withTableBorder=True,
                    withColumnBorders=True,
                )
                
                return html.Div([
                    dmc.Alert(
                        f"Showing {min(len(df), 100)} of {len(df)} rows from '{table_name}'",
                        color="blue",
                        mb="md",
                    ),
                    dmc.ScrollArea(table, h=600),
                ])
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {e}")
            return dmc.Alert(
                title="Error",
                children=f"Failed to load table '{table_name}': {str(e)}",
                color="red",
            )
