"""MCP distribution layer for the read-only NYC Project Signals dataset.

The default stdio transport is suitable for local MCP clients. Streamable HTTP
is available for a separately hosted, authenticated deployment; this module
does not add authentication or multi-tenant isolation by itself.
"""
from __future__ import annotations

import csv
import contextlib
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .core import DB, connect, get_state
from .pipeline import filtered, ingest, load_projects, write_csv
from .quality import report


def _refresh_needed(database, max_age_hours=24):
    path = Path(database)
    if not path.exists():
        return True
    conn = connect(path)
    try:
        states = get_state(conn)
        successes = [s.get('last_success') for s in states.values() if s.get('last_success')]
        if not successes:
            return True
        newest = max(datetime.fromisoformat(value.replace('Z', '+00:00')) for value in successes)
        return datetime.now(timezone.utc) - newest > timedelta(hours=max_age_hours)
    finally:
        conn.close()


def refresh_if_stale(database=DB, max_age_hours=24):
    """Refresh official sources once per stale window, safely across MCP processes."""
    if os.environ.get('NYC_SIGNALS_NO_AUTO_REFRESH') == '1' or not _refresh_needed(database, max_age_hours):
        return {'refreshed': False, 'reason': 'fresh or disabled'}
    lock_path = Path(database).with_suffix('.refresh.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import fcntl
        with lock_path.open('w') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {'refreshed': False, 'reason': 'another process is refreshing'}
            if not _refresh_needed(database, max_age_hours):
                return {'refreshed': False, 'reason': 'another process refreshed it'}
            conn = connect(database)
            try:
                # Ingestion is public, bounded, and never calls Azure inference.
                with contextlib.redirect_stdout(sys.stderr):
                    result = ingest(conn, days=90, limit=10000, force=False)
                failures = [name for name, state in result['sources'].items() if state.get('status') == 'error']
                return {'refreshed': True, 'failed_sources': failures}
            finally:
                conn.close()
    except ImportError:
        # Windows has no fcntl; avoid hiding the server behind an optional lock.
        return {'refreshed': False, 'reason': 'refresh lock unavailable'}


def build_server(database=DB, auto_refresh=True) -> FastMCP:
    mcp = FastMCP(
        'NYC Project Signals',
        instructions=(
            'Search official NYC public-record project signals. Treat every result as a research candidate, '
            'not a confirmed buyer, opening, contact, or purchasing need. Always cite the supplied official source URLs '
            'and preserve the limitations in the result. Do not invent contact details or buying intent.'
        ),
    )

    def projects():
        if auto_refresh:
            refresh_if_stale(database)
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
        if auto_refresh:
            refresh_if_stale(database)
        conn = connect(database)
        try:
            return get_state(conn)
        finally:
            conn.close()

    @mcp.tool()
    def refresh_status() -> dict:
        """Show source freshness and whether the next startup will refresh public data."""
        if auto_refresh:
            refresh_if_stale(database)
        conn = connect(database)
        try:
            states = get_state(conn)
            return {'auto_refresh_window_hours': 24, 'sources': {
                name: {'status': state.get('status'), 'last_success': state.get('last_success'),
                       'complete': state.get('complete'), 'fetched': state.get('fetched'),
                       'source_count': state.get('source_count')}
                for name, state in states.items()
            }}
        finally:
            conn.close()

    

    @mcp.tool()
    def quality_status() -> dict:
        """Return measured quality gates; unmeasured accuracy remains null."""
        if auto_refresh:
            refresh_if_stale(database)
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


def run(database=DB, transport='stdio', host='127.0.0.1', port=8000, auto_refresh=True):
    if auto_refresh:
        refresh_if_stale(database)
    server = build_server(database, auto_refresh=auto_refresh)
    if transport == 'stdio':
        server.run(transport='stdio')
    elif transport in ('streamable-http', 'sse'):
        server.settings.host = host
        server.settings.port = port
        server.run(transport=transport)
    else:
        raise ValueError('Transport must be stdio, streamable-http, or sse.')
