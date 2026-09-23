# NYC Project Signals

A working local research product for commercial construction and restaurant activity across NYC's five boroughs. It collects official records, preserves evidence, groups filings by job number, and exports portable data. It does not claim a permit or license application is a confirmed sales lead.

## Open the app

Requires Python 3.11+. No third-party Python or JavaScript packages are required.

```sh
cd /Users/krkaushikkumar/Desktop/money
python3 -m nyc_signals serve
```

Open http://127.0.0.1:8765. The server binds only to localhost. It is not a public multi-tenant service. Search, feed, borough and date filters, source histories, business roles, property context, browser-local bookmarks, and filtered CSV downloads are functional. Saved items remain in this browser; they are not user accounts.

## Collect and update

```sh
# Small pilot: up to 200 rows per source
python3 -m nyc_signals ingest --days 90 --limit 200

# Bounded collection: explicitly reports truncated sources
python3 -m nyc_signals ingest --days 90 --limit 10000

# Add property context to up to 500 relevant tax lots
python3 -m nyc_signals enrich --limit 500

# Save a complete, restorable local export
python3 -m nyc_signals export
python3 -m nyc_signals quality
python3 -m nyc_signals sample --size 100
```

Use `--sources dob_applications dob_permits` to update selected sources. `--force` refreshes even unchanged datasets. Optional `SOCRATA_APP_TOKEN` raises public API rate limits. Partial failures retain earlier data and exit with code 2; inspect Sources & coverage. No unattended scheduling is installed. Run ingestion daily while validating demand.

Ingestion is incremental in processing: source metadata skips unchanged complete queries, raw content hashes avoid reprocessing unchanged records, and changed versions are retained. Updated datasets are re-read within the chosen window to catch amendments; this is not an API-level change feed. Pending liquor licenses use a current NYC snapshot; missing rows are archived only after a complete successful snapshot. They are never assumed approved.

## Sources and identity

| Source | Dataset | Window field |
|---|---|---|
| DOB NOW applications | `w9ak-ipjd` | Current status date |
| DOB NOW permits | `rbx6-tga4` | Issue date |
| Legacy applications | `ic3t-wcy2` | Latest action date |
| Legacy permits | `ipu4-2q9a` | Issue date |
| Occupancy | `pkdm-hqz6` | Submission date; display issuance where provided |
| NY pending liquor licenses | `f8i8-k2gm` | Current NYC-county snapshot |
| PLUTO | `64uk-42ks` | Targeted BBL lookup |

DOB records are grouped by the official root job number. Distinct jobs at one building are kept separate. Same-site links are explicitly unverified research hints. A BBL identifies a tax lot, not necessarily one building; the BIN is stored separately. Licenses remain separate from DOB projects. Corporate owner, filing applicant, permittee, representative and license applicant roles remain distinct. Personal names and phone fields are excluded from customer-facing views; source raw records are retained locally for audit and may contain public personal fields.

Scope is the fetched query window and configured row cap. Missing specialized electrical/elevator/LAA feeds, legacy occupancy records, community-board documents, and permits outside the window mean this is not comprehensive citywide coverage. No independently verified contacts, supplier availability, opening dates or buying-intent scores are invented. Source terms must be reviewed for the intended redistribution before launch.

## Review quality

`exports/review-sample.csv` contains a deterministic, stratified sample. Inspect evidence, fill `verdict` (`correct` or `incorrect`), `expected_feeds` (semicolon-separated), `reviewer`, `kind` (`human` or `agent`), and notes.

```sh
python3 -m nyc_signals review-import exports/review-sample.csv
python3 -m nyc_signals quality
```

Review results are tied to evidence hashes and become stale when evidence changes. Agent/model evaluation is never counted as human review. The launch report requires 100 human-reviewed projects overall, 30 reviewed examples per feed, >=90% accuracy, and 30 recent projects per offered feed. Payment setup and source-reuse review remain mandatory operator launch decisions. Billing is deliberately unavailable while these gates are unmet; there is no pretend checkout or active subscription to cancel.

## Azure credits and bounded inference

Public data collection and the local app incur no Azure charges. Optional Azure classification review is implemented but **disabled by default**. It provides a second opinion and does not silently overwrite source facts or measured quality.

1. Verify the credit balance, pending usage, exact cutoff (with timezone), model eligibility and model prices in your Azure account.
2. Copy `config/azure-budget.example.json` to `data/azure-budget.json` and fill verified facts. Initial maximum is $50; total configured ceiling may never exceed $700 or available credit less the pending-usage reserve.
3. Use a pre-existing eligible deployment. Authenticate with Azure CLI (Entra inference permission required) or set `AZURE_OPENAI_API_KEY` in your environment. Never put credentials in code or exports.
4. Run `python3 -m nyc_signals azure-review --limit 20`.

The runner requires billing verification within two hours, reserves conservative token cost before requests using a SQLite transaction, caps completion tokens, retains reservations on failure, does not automatically retry paid requests, and stops 60 minutes before expiry. Actual Azure billing is authoritative; the local ledger does not cap unrelated resources in your subscription. This app creates no Azure resources and never deletes existing resources.

## Exports and recovery

`exports/latest/` contains all three CSV feeds, all-project CSV, an evidence-prioritized `leads.csv` work queue, full project JSON, source status JSON, a consistent SQLite backup, and a SHA-256 manifest. Public CSV cells are neutralized against spreadsheet formula injection. Backups contain raw evidence, so share the customer CSV files rather than the entire database when appropriate.

```sh
python3 -m nyc_signals --db exports/latest/signals.sqlite3 serve --port 8766
```

Code, data and prompts are local and remain usable without Azure. Estimate costs from actual deltas before enabling cloud hosting. A full cloud service, identity system, payment onboarding and domain publication are later launch work; the approved first deliverable is a local searchable demo and portable dataset.

## Checks

```sh
python3 -m unittest discover -s tests -v
node --check web/app.js
```

Tests cover identity grouping, date conversion, preservation of multi-work-type permits, idempotence, source failures, snapshot reconciliation, review invalidation, CSV injection, filtering, and backup recovery.

## Local HTTP interface

Read-only endpoints: `/api/health`, `/api/summary`, `/api/projects`, `/api/leads`, `/api/projects/{id}`, `/api/sources`, `/api/quality`, and `/api/export.csv`. Project and export filters are `feed`, `borough`, `q`, `since`, `until`, `review`, and `priority`. `/api/leads` defaults to high-priority evidence candidates. Priority is a transparent research-work ranking based on source recency, explicit project language, business roles, property context, and negative evidence; it is not a conversion probability or buying-intent score. JSON pagination uses `offset` and `limit` (maximum 200). No browser route triggers cloud processing or data mutations.

## MCP distribution

The same read-only dataset is available as MCP tools. Install the project in an isolated environment with Python 3.11+:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m nyc_signals --db data/signals.sqlite3 mcp
```

Configure a local MCP client to launch the command with the repository as its working directory:

```json
{
  "mcpServers": {
    "nyc-project-signals": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["-m", "nyc_signals", "--db", "data/signals.sqlite3", "mcp"]
    }
  }
}
```

Tools are `search_leads`, `lead_digest`, `workflow_playbook`, `get_project`, `list_sources`, `refresh_status`, `quality_status`, and `export_leads`. Search and export accept service categories such as Restaurant equipment, Commercial cleaning, Signage, and Building services. They return official source URLs and limitations. On startup, stdio refreshes the public NYC sources when the local database is missing or older than 24 hours; use `--no-refresh` or `NYC_SIGNALS_NO_AUTO_REFRESH=1` to disable this. The refresh is bounded to the configured 90-day window and never calls Azure inference. The stdio server is local and free. `--transport streamable-http` is available for a hosted deployment, but do not expose it publicly without authentication, rate limiting, tenant isolation, and a terms/privacy review.
