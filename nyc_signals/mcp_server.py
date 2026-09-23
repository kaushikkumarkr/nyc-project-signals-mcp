"""MCP distribution layer for the read-only NYC Project Signals dataset.

The default stdio transport is suitable for local MCP clients. Streamable HTTP
is available for a separately hosted, authenticated deployment; this module
does not add authentication or multi-tenant isolation by itself.
"""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .core import DB, connect, get_state
from .pipeline import filtered, load_projects, write_csv
from .quality import report


def build_server(database=DB) -> FastMCP:
    mcp = FastMCP(
        'NYC Project Signals',
        instructions=(
            'Search official NYC public-record project signals. Treat every result as a research candidate, '
            'not a confirmed buyer, opening, contact, or purchasing need. Always cite the supplied official source URLs '
            'and preserve the limitations in the result. Do not invent contact details or buying intent.'
        ),
    )

    def projects():
        conn = connect(database)
        try:
            return load_projects(conn)
        finally:
            conn.close()

    @mcp.tool()
    def search_leads(
        query: str = '',
        feed: str = 'all',
        borough: str = 'all',
        priority: str = 'high',
        since: str = '',
        limit: int = 20,
    ) -> dict:
        """Find evidence-prioritized NYC project candidates for a sales or research workflow.

        Priority is a transparent research-work ranking, not a conversion probability.
        Dates must be YYYY-MM-DD. Results include official source URLs and limitations.
        """
        limit = min(max(int(limit), 1), 50)
        params = {'q': query, 'feed': feed, 'borough': borough, 'priority': priority, 'since': since}
        matches = filtered(projects(), params)
        return {
            'purpose': 'Evidence-prioritized research candidates; verify before outreach.',
            'total': len(matches),
            'results': matches[:limit],
        }

    @mcp.tool()
    def get_project(project_id: str) -> dict:
        """Retrieve one project, its source history, roles, property context, and limitations."""
        project = next((p for p in projects() if p['id'] == project_id), None)
        return project or {'error': 'Project not found', 'project_id': project_id}

    @mcp.tool()
    def list_sources() -> dict:
        """List source datasets, freshness, row limits, and known collection gaps."""
        conn = connect(database)
        try:
            return get_state(conn)
        finally:
            conn.close()

    @mcp.tool()
    def quality_status() -> dict:
        """Return measured quality gates; unmeasured accuracy remains null."""
        conn = connect(database)
        try:
            return report(conn)
        finally:
            conn.close()

    @mcp.tool()
    def export_leads(
        query: str = '',
        feed: str = 'all',
        borough: str = 'all',
        priority: str = 'high',
        since: str = '',
        limit: int = 200,
    ) -> dict:
        """Return a bounded CSV export for downstream CRM or automation testing."""
        limit = min(max(int(limit), 1), 500)
        matches = filtered(projects(), {'q': query, 'feed': feed, 'borough': borough,
                                        'priority': priority, 'since': since})[:limit]
        stream = io.StringIO(newline='')
        write_csv(matches, stream)
        return {'purpose': 'Evidence-prioritized candidates; verify before outreach.',
                'count': len(matches), 'csv': stream.getvalue()}

    return mcp


def run(database=DB, transport='stdio', host='127.0.0.1', port=8000):
    server = build_server(database)
    if transport == 'stdio':
        server.run(transport='stdio')
    elif transport in ('streamable-http', 'sse'):
        server.settings.host = host
        server.settings.port = port
        server.run(transport=transport)
    else:
        raise ValueError('Transport must be stdio, streamable-http, or sse.')

