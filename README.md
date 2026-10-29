# OpenNSE Quant & Macro Terminal

A self-hosted analytics terminal for Nairobi Securities Exchange (NSE) equities and Kenyan
macroeconomic data, built only on public sources: the NSE's public market-statistics web page,
Central Bank of Kenya (CBK) statistical releases, PDF financial statements and CSV/Excel files. It
ships as two deliverables that run standalone or mount into an existing FastAPI + Next.js +
PostgreSQL platform:

- **`opennse_terminal`** (`backend/`): a Python package holding one FastAPI `APIRouter` under
  `/api/v1/terminal`, the ingestion pipelines, a scheduler process, an isolated Alembic migration
  history and a deterministic seeder.
- **A Next.js App Router application** whose widgets live in the self-contained folder
  `src/components/terminal/`, which a host application can import.

## Features

**Ingestion and normalization**

- NSE daily closing prices, volumes and estimated turnover, scraped from the public
  market-statistics page.
- CBK macro series: 12-month inflation, 91/182/364-day T-bill yields, Treasury bond yields, the
  interbank rate, the Central Bank Rate, KES/USD and FX reserves.
- PDF line-item extraction from filings and IPO prospectuses: revenue, EBITDA, net income, total
  debt, operating cash flow and shares outstanding.
- CSV/Excel import of equities, prices, fundamentals, macro series and foreign flows.
- A scheduler process, idempotent upserts, a run record per run, and layout-change detection that
  writes nothing when a source changes shape.

**Quantitative and cycle analytics**

- P/E, P/B, EV/EBITDA and dividend yield per counter and per sector, plus historical valuation
  bands.
- Rolling volume and turnover Z-score spike detection, a foreign-versus-local net flow estimator
  and sector rotation returns.
- An event and election cycle backtester (2013, 2017, 2022 and 2027 by default) with inflation and
  T-bill overlays, and a labelled projection for cycles still in progress or in the future.
- An IPO sandbox: DCF, comparable multiples, a blended fair value against the offer price, and a
  WACC × EBITDA-margin sensitivity matrix.

**Terminal views**

| Route | View |
|---|---|
| `/` | Macro and market overview: indicator tiles, yield curve, inflation trajectory, sector rotation heatmap |
| `/cycles` | Election and cycle backtesting: multi-cycle overlay with valuation-floor and volume-exhaustion markers |
| `/ipo` | IPO valuation sandbox: sliders with server-computed fair value and the sensitivity matrix |
| `/screener` | Screener, watchlists, anomaly feed, valuation bands and net flows for the selected counter |

**Stack.** Python 3.12 (`requires-python >=3.11`), FastAPI 0.162.0, Pydantic 2.14.1, SQLAlchemy
2.0.55 (async) with asyncpg 0.32.0, Alembic 1.20.1, APScheduler 3.11.3, PostgreSQL 17 (development
container; 14 or later supported). Next.js 16.4.3 (App Router), React 19.3.1, TypeScript 5.9.3,
Tailwind CSS 4.3.3, Recharts 3.10.1, Node 24 LTS. Every dependency is pinned exactly in
`backend/pyproject.toml` and `package.json`.

## Quick start

Prerequisites: Python 3.12, Node 24 LTS, and Docker with Compose v2.

Backend, from the repository root:

```bash
cp .env.example .env
docker compose up -d --wait postgres && docker compose exec postgres pg_isready -U opennse
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e "./backend[dev]"
opennse-terminal migrate upgrade head
python -m opennse_terminal.seed.seed_terminal_db      # or: opennse-terminal seed
uvicorn opennse_terminal.main:app --port 8000
```

Frontend, in a second terminal from the repository root:

```bash
npm ci
npm run build && npm run start                         # http://localhost:3000
```

Use `npm run dev` instead of `build` and `start` while developing.

- The seeder is idempotent: a second run inserts 0 data rows and records only its own run. It
  exits with code 2 and "run `opennse-terminal migrate upgrade head` first" when the schema is not
  at the migration head.
- `TERMINAL_API_ORIGIN` (default `http://localhost:8000`) is the FastAPI origin to which the
  Next.js rewrites forward `/api/v1/terminal/*`. Next.js evaluates `rewrites()` at `next build`
  (and `next dev`), so set it before building; changing it later needs a rebuild.
- `python -m playwright install --with-deps chromium` is needed only to run the live
  `nse_market_statistics` pipeline. The tests run on recorded fixtures.
- The API's OpenAPI documentation is served at `http://localhost:8000/docs`.

## Verification

From the repository root, with the virtual environment active:

```bash
# Packaging: the wheel must carry the migrations and the exchange-closure data
pip wheel ./backend --no-deps -w dist
python - <<'EOF'
import glob, sys, zipfile
names = set(zipfile.ZipFile(glob.glob("dist/opennse_terminal-*.whl")[0]).namelist())
need = [f"opennse_terminal/{p}" for p in ("migrations/env.py", "migrations/script.py.mako",
        "migrations/versions/terminal_0001_initial_schema.py", "ingestion/data/nse_closures.csv")]
sys.exit([p for p in need if p not in names] or 0)
EOF

# Backend: lint, types, tests and coverage gates
ruff check backend && ruff format --check backend
mypy --config-file backend/pyproject.toml backend/opennse_terminal
pytest backend/tests --cov=opennse_terminal --cov-fail-under=80
coverage report --include="*/quant/*" --fail-under=95

# Frontend: types, lint and production build
npm run typecheck && npm run lint && npm run build

# End to end: needs the API on :8000 against the migrated and seeded database
npx playwright install --with-deps chromium
npm run test:e2e
```

- The wheel check exits non-zero and prints any missing resource.
- `backend/tests/unit/quant/` holds one test file per quantitative method, asserting hand-computed
  values and edge cases such as zero or negative earnings, missing volume days and WACC at or below
  terminal growth.
- mypy does not discover a configuration file below the working directory, so `--config-file` is
  required; without it, strict mode and the import overrides would silently not apply. ruff and
  pytest find `backend/pyproject.toml` from their target paths.
- `pytest` skips `live` tests by default (`addopts = "-m 'not live' ..."`). Run
  `pytest backend/tests -m live` to hit the real public sources.
- Integration tests need PostgreSQL at `TERMINAL_TEST_DATABASE_URL`. They use its administrative
  database `opennse_test` (created by `docker/postgres-init.sql`) only to create and drop their own
  template and clone databases, so the role needs `CREATEDB`. They do not depend on the seed steps
  above.
- `npm run lint` runs ESLint, then `node scripts/check-terminal-boundary.mjs` (the widget import
  boundary), then the same script with `--self-test`.
- `npm run test:e2e` starts the standalone app on port 3000 and the host fixture on port 3100. It
  never starts the API or the database, and it expects `npm run build` to have used the same
  `TERMINAL_API_ORIGIN`. The Playwright run and the host fixture accept only a loopback origin
  (`localhost`, `127.0.0.0/8` or `[::1]`, any port) without userinfo, a query or a fragment:
  otherwise Playwright stops before starting either server, and the fixture exits when it loads
  its config. The standalone `next.config.ts` still accepts remote origins.

## Architecture

The backend is a layered modular monolith; the frontend is a component library plus four pages.

- **`api/routes`** validates input and resolves dependencies, then calls **`services`**, which
  query PostgreSQL, call the **`quant`** engine (pure `Decimal` functions, no I/O), round once and
  build the Pydantic response.
- **`ingestion`** pipelines run fetch → parse → validate → upsert → record. A run writes through
  `db/upsert.py` in one transaction, holds a transaction-scoped PostgreSQL advisory lock, and is
  recorded in `terminal_ingestion_runs`.
- The **seeder** writes through the ORM models.
- The **widgets** call the API through `src/components/terminal/lib/apiClient.ts`, which shares one
  in-flight request among identical concurrent `GET`s.

```mermaid
flowchart LR
    NSE[NSE market-statistics page] --> PIPES[Ingestion pipelines]
    CBK[CBK releases: HTML, CSV, PDF] --> PIPES
    FILES[CSV/Excel files] --> CLI[opennse-terminal CLI]
    SCHED[Scheduler process] --> PIPES
    CLI --> PIPES
    PIPES --> DB[(PostgreSQL<br/>terminal_ tables)]
    SEEDER[Seeder] --> DB
    PDFS[Filing PDFs] --> ROUTER
    WIDGETS[src/components/terminal widgets] --> ROUTER[terminal_router<br/>/api/v1/terminal]
    ROUTER --> SERVICES[Services]
    SERVICES --> QUANT[Quant engine<br/>pure Decimal]
    SERVICES --> DB
```

| Process | Command | Notes |
|---|---|---|
| API | `uvicorn opennse_terminal.main:app --port 8000` | Stateless; additional workers are safe |
| Scheduler | `opennse-terminal scheduler` | Exactly one dedicated process; never started inside web workers, where every job would run once per worker |
| Web | `npm run start` | Next.js server; same-origin rewrites to the API |

Repository layout:

```text
backend/
  opennse_terminal/      api/ services/ quant/ ingestion/ models/ schemas/ db/ migrations/ seed/
  tests/                 unit/ integration/ fixtures/ helpers/
src/
  app/                   standalone pages: /, /cycles, /ipo, /screener
  components/terminal/   embeddable widgets, the index.ts barrel, lib/ and shared/
scripts/                 check-terminal-boundary.mjs (widget import boundary)
e2e/                     Playwright specs and host-fixture/ (a minimal foreign host app)
docker/                  postgres-init.sql (creates opennse_test)
```

### Numeric conventions

- Currency defaults to KES: every `currency` column is `CHAR(3) NOT NULL DEFAULT 'KES'`, and
  analytics refuse mismatched currencies with the flag `currency_mismatch`.
- Money and ratios are PostgreSQL `NUMERIC` and Python `Decimal`; no float enters a calculation.
- Values are rounded once, at the response boundary, with `ROUND_HALF_UP`: 2 decimal places for
  KES, prices, ratios, percentages and Z-scores, 6 for discount factors.
- JSON carries decimals as strings, already rounded (`"offer_price_kes": "7.50"`). Requests accept a
  string or a number. The widgets format the string itself and convert to a number only for chart
  coordinates.
- Share counts and volumes are JSON integers of at most 10^12, so they stay exact in JavaScript.
- An undefined metric is `null` with reason codes in `flags` (for example `pe_negative_earnings`,
  `ev_not_meaningful_for_banks`), never zero.
- Units: `*_pct` columns are percentage points (`6.80` means 6.80%), `*_kes` columns are whole KES,
  `fx_kes_usd` is KES per USD and `fx_reserves_usd_m` is millions of USD.

## API

All 16 endpoints are grouped under one `APIRouter`, `terminal_router`, with the prefix
`/api/v1/terminal`. Paths below are relative to it. The OpenAPI documentation (`/docs` in the
standalone app) lists every parameter and schema.

| Method and path | Purpose |
|---|---|
| `GET /health` | Status, database connectivity and version (503 `database_unavailable` when the database is down) |
| `GET /macro/overview` | Latest CBK indicators, inflation and rates history, and the current and historical yield curve |
| `GET /market/screener` | Per-counter multiples, dividend yield and Z-scores with filters and sorting, plus sector aggregates |
| `GET /market/sector-heatmap` | Capitalization-weighted sector returns per month or week, and relative to the market |
| `GET /market/valuation-bands` | Historical P/E or P/B bands of one counter or one sector |
| `GET /market/anomalies` | Recent positive volume and turnover Z-score spikes |
| `GET /market/flows` | Foreign versus local net flows of a counter, a sector or the market |
| `GET /cycles/backtest` | Election and event cycle overlay with markers, summary, macro overlays and projections |
| `GET /ipo/scenarios` | Read-only IPO sandbox presets |
| `POST /ipo/valuation` | DCF, comparables, blended fair value against the offer, and the sensitivity matrix |
| `POST /ingest/pdf` | Extract line items from a filing or prospectus PDF; optionally commit them to fundamentals |
| `GET /watchlists` | The caller's watchlists |
| `POST /watchlists` | Create a watchlist (201) |
| `DELETE /watchlists/{watchlist_id}` | Delete a watchlist (204) |
| `PUT /watchlists/{watchlist_id}/items/{ticker}` | Add a ticker (idempotent) |
| `DELETE /watchlists/{watchlist_id}/items/{ticker}` | Remove a ticker (idempotent) |

**Errors** arrive in one of two shapes:

- Coded errors, for business and cross-field rules and unknown entities (404 `unknown_ticker`,
  `unknown_sector`, `watchlist_not_found`):
  `{"detail": {"code": "primary_exceeds_offer_size", "message": "...", "field": "primary_proceeds_kes"}}`.
  `field` names the offending parameter, or is `null`.
- Single-field type and range failures: FastAPI's standard 422 body, where `detail` is a list of
  `{type, loc, msg, input}` entries with no stable code.

Error bodies never carry result data.

**PDF uploads** (`POST /ingest/pdf`, multipart: `file`, `document_type`, `ticker`, `company_name`,
`period_end_date`, `period_type`, `items`, `commit`) are bounded as follows:

- The whole request is limited to `TERMINAL_PDF_MAX_BYTES` plus 1 MiB before multipart parsing
  (413 `request_too_large`). The guard is `TerminalUploadRoute`, the ingest sub-router's route
  class, so it travels with `terminal_router` into a host.
- The file itself may be at most `TERMINAL_PDF_MAX_BYTES` (20 MiB, 413 `file_too_large`), must
  start with `%PDF-` (415 `not_a_pdf`), must not be encrypted or unreadable (422 `pdf_encrypted`,
  `pdf_unreadable`) and may have at most 400 pages (`TERMINAL_PDF_MAX_PAGES`, 422
  `too_many_pages`).
- At most 2 parses run per process (`TERMINAL_PDF_MAX_CONCURRENCY`); a further request gets 429
  `ingest_busy` with `Retry-After: 5`. Parsing runs in a worker thread, and the raw bytes are never
  stored.
- `commit=false` (the default) returns the full extraction as a preview. `commit=true` also writes
  the `found` and `derived` items, never `ambiguous` ones, into `terminal_fundamentals`. It requires
  an existing `ticker`, a `period_end_date` and a KES statement (422
  `commit_requires_ticker_and_period` or `commit_requires_kes`, 404 `unknown_ticker`, 409
  `currency_conflict`); a failed commit writes neither the filing nor the fundamentals row.

**Rate limiting** is not built in beyond the PDF guards; it belongs to the host's gateway or
reverse proxy.

## Host integration: backend

Install the package into the host's environment, from a checkout or a git URL:

```bash
pip install ./backend
```

Mount every endpoint with one line:

```python
from opennse_terminal.api import terminal_router

app.include_router(terminal_router)
```

Collision safety:

- Every path lives under `/api/v1/terminal/`.
- Every OpenAPI `operationId` is prefixed `terminal_`, so the host's schema has no duplicates.
- The router registers no middleware, exception handlers, startup or shutdown hooks, CORS settings
  or global state. Errors are raised as `HTTPException` with the coded body above.

Authentication is optional and pluggable. Every external dependency is a FastAPI dependency the host
can replace through `app.dependency_overrides`; all are exported from `opennse_terminal.api`:

| Override target | Default | Host replacement |
|---|---|---|
| `get_terminal_settings` | Cached `TerminalSettings` from `TERMINAL_*` variables and `.env` | The same variables, or a zero-argument `lambda: TerminalSettings(...)` built from the host's config |
| `get_terminal_session` | A lazily created async engine and one `AsyncSession` per request | The host's provider yielding a SQLAlchemy `AsyncSession` on the same PostgreSQL database through asyncpg. Write routes call `await session.commit()` themselves |
| `get_terminal_principal` | `TerminalPrincipal(subject="local", is_authenticated=False)` | Map the host's user to `TerminalPrincipal(subject=..., is_authenticated=True)`, or raise 401. The router depends on it, so it protects every terminal endpoint, `/health` included. Watchlists are owned by `subject` (1–255 characters) |
| `require_ingest_permission` | Allows every caller | Raise 403 unless the caller may ingest PDFs, for example an administrator |

`dispose_terminal_engine()` releases the default engine on shutdown; it is needed only when the host
keeps the default session provider.

```python
from fastapi import Depends, HTTPException

from opennse_terminal.api import (
    TerminalPrincipal,
    get_terminal_principal,
    get_terminal_session,
    require_ingest_permission,
)


async def host_terminal_principal(user=Depends(get_current_user)) -> TerminalPrincipal:
    # get_current_user is the host's own dependency; it raises 401 for anonymous callers.
    return TerminalPrincipal(subject=str(user.id), is_authenticated=True)


async def host_ingest_permission(user=Depends(get_current_user)) -> None:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="administrators only")


app.dependency_overrides[get_terminal_principal] = host_terminal_principal
app.dependency_overrides[require_ingest_permission] = host_ingest_permission
app.dependency_overrides[get_terminal_session] = host_get_async_session
```

Migrations, ingestion, the scheduler and the seeder run outside FastAPI. Point them at the host
database with `TERMINAL_DATABASE_URL`:

```bash
TERMINAL_DATABASE_URL=postgresql+asyncpg://USER:PASSWORD@HOST:5432/DATABASE opennse-terminal migrate upgrade head
```

## Migration isolation

Every table, index and constraint, and the Alembic version table, carries the `terminal_` prefix:

- The models use their own declarative base, `TerminalBase`, whose naming convention puts the table
  name into every index and constraint name (for example `uq_terminal_equities_ticker`).
- The history is recorded in `terminal_alembic_version`, never the host's `alembic_version`.
  Revision IDs are `terminal_0001`, `terminal_0002` and so on.
- An `include_name` filter admits only `terminal_` tables, so autogenerate never reflects, alters or
  proposes to drop a host table.
- There is no `alembic.ini`. Run `opennse-terminal migrate upgrade head` or
  `opennse-terminal migrate downgrade <rev>`; the Alembic configuration is built in code.
- `downgrade base` drops only the 11 `terminal_` tables the history created; the then-empty
  `terminal_alembic_version` table remains.

The 11 tables are `terminal_equities`, `terminal_daily_prices`, `terminal_macro_indicators`,
`terminal_bond_yields`, `terminal_fundamentals`, `terminal_corporate_filings`,
`terminal_ipo_scenarios`, `terminal_cycles`, `terminal_watchlists`, `terminal_watchlist_items` and
`terminal_ingestion_runs`. No users, accounts, sessions, roles or auth tables are created;
watchlists are keyed by the principal's opaque `subject`.

**Host-side step.** A host whose own Alembic uses autogenerate must add the mirror filter to its
`env.py`. Otherwise the host's autogenerate would propose dropping the terminal tables:

```python
def include_name(name, type_, parent_names):
    if type_ == "table":
        return not (name or "").startswith("terminal_")
    return True


context.configure(
    connection=connection,
    target_metadata=target_metadata,
    include_name=include_name,
)
```


## Host integration: frontend

The widgets are client components with explicit props and no host dependencies, in the single
folder `src/components/terminal/`.

1. Copy `src/components/terminal/` into the host, for example into its own
   `src/components/terminal/`, and import from the folder's `index.ts` barrel.
2. Install the runtime dependencies: `recharts@3.10.1` and the `react-is` version that matches the
   host's React. React 18.2 or later is supported.
3. Make Tailwind scan the folder (below), and route the API (below).

```tsx
import { YieldCurveChart } from "@/components/terminal";

export default function MacroPage() {
  return <YieldCurveChart compareMonthsAgo={[6, 12]} />;
}
```

Every data widget takes the common props `apiBaseUrl?: string` (default `"/api/v1/terminal"`),
`getHeaders?: () => HeadersInit | Promise<HeadersInit>` (host auth, such as a bearer token) and
`className?: string`. Widgets that accept a `data` prop skip fetching when the host passes it.

| Widget | Widget-specific props (default) |
|---|---|
| `MacroSnapshot` | `historyMonths` (24), `data` |
| `YieldCurveChart` | `historyMonths` (24), `compareMonthsAgo` ([6, 12]), `data` |
| `InflationTrajectoryChart` | `historyMonths` (24), `showTargetBand` (true), `data` |
| `SectorRotationHeatmap` | `interval`: `"month"` or `"week"` (`"month"`), `periods` (12), `data` |
| `CycleBacktester` | `defaultEventDates` (server defaults), `defaultWindow`: `"180d"` or `"12m"` (`"180d"`), `tickers` |
| `IpoValuationSandbox` | `scenarioName` (first preset), `debounceMs` (150) |
| `EquityScreener` | `initialFilters`, `onSelectTicker(ticker)` |
| `WatchlistManager` | `onWatchlistsChange(lists)` |
| `ValuationBandsChart` | exactly one of `ticker` or `sector`, `metric`: `"pe"` or `"pb"` (`"pe"`), `lookbackYears` (5) |
| `NetFlowChart` | `ticker` or `sector` (neither means the whole market), `rollingWindow` (20) |
| `AnomalyFeed` | `metric`: `"volume"`, `"turnover"` or `"both"` (`"both"`), `threshold` (2), `onSelectTicker(ticker)` |

`SensitivityMatrix` is the presentational matrix that `IpoValuationSandbox` renders. It never
fetches, and takes `data` (the `sensitivity` object of a `POST /ipo/valuation` response),
`offerPriceKes`, an optional `selected: { waccPct, ebitdaMarginPct }` and `className`.

Every widget root carries `data-testid="terminal-<widget-kebab-name>"` (for example
`terminal-yield-curve-chart`) and `data-state="loading|ready|empty|error"`.

**What the widgets never depend on:** host global state, React context providers or data
libraries; the host's layout, fonts or body styles; global CSS files; `next/*` imports; the `@/`
path alias; or any import that resolves outside `src/components/terminal/`. The only packages they
import are `react`, `react-dom` and `recharts`. `scripts/check-terminal-boundary.mjs`, run by
`npm run lint`, enforces this on resolved import paths.

**Tailwind.**

- Tailwind 4 host: if the folder is inside the host project, automatic source detection already
  covers it. If it is consumed from outside the project root or from `node_modules`, add
  `@source "<relative-path>/src/components/terminal";` to the host's CSS entry file.
- Tailwind 3 host: add `"./src/components/terminal/**/*.{ts,tsx}"` (adjusted to the folder's
  location) to `content` in `tailwind.config`.
- The widgets use only the default palette (slate, emerald, rose, amber, sky). A host that replaces
  the default colours must keep those families.

**Routing the API.** Either proxy `/api/v1/terminal/*` from the host's Next.js app to the host's
FastAPI app that mounts `terminal_router`, or pass an absolute `apiBaseUrl` and allow that origin in
the host's CORS settings. The standalone app needs no CORS: its `next.config.ts` rewrites the
browser's same-origin calls to `TERMINAL_API_ORIGIN`.

`e2e/host-fixture/` is a working minimal host: its own layout and CSS, one `@source` line, and a page
that imports `YieldCurveChart` from the terminal folder.

## CLI and scheduling

The `opennse-terminal` console script reads the database from `TERMINAL_DATABASE_URL` (or `.env` in
the working directory). `opennse-terminal <command> --help` documents each command.

```text
opennse-terminal migrate upgrade head                 # or any revision; migrate downgrade <rev>
opennse-terminal seed [--database-url URL]            # same as python -m opennse_terminal.seed.seed_terminal_db
opennse-terminal ingest <pipeline>                    # one pipeline, or ingest cbk for all five CBK pipelines
opennse-terminal ingest nse_market_statistics --trade-date YYYY-MM-DD
opennse-terminal ingest cbk_reserves --limit N        # backfill the newest N weekly bulletins (1-26)
opennse-terminal import <kind> <path> [--sheet NAME]
opennse-terminal scheduler
```

**Pipelines:** `nse_market_statistics`, `cbk_rates_csv`, `cbk_inflation`, `cbk_fx`, `cbk_bonds` and
`cbk_reserves`. `ingest cbk` runs the five CBK pipelines in that order, each as its own run, and
exits with the code of the first that did not exit 0. `--trade-date` resolves an
`ambiguous_session` NSE run by stating the session the page shows.

**Import kinds**, from a `.csv` or `.xlsx` file of at most 50 MiB and 1,000,000 rows:

| Kind | Required columns | Target and key |
|---|---|---|
| `equities` | `ticker`, `company_name`, `sector`, `shares_outstanding` | `terminal_equities` on `ticker` |
| `daily_prices` | `ticker`, `trade_date`, `close_price`, `volume` | `terminal_daily_prices` on (`ticker`, `trade_date`); overwrites prices from any source; a missing turnover is estimated as volume × close |
| `fundamentals` | `ticker`, `period_end_date` | `terminal_fundamentals` on (`ticker`, `period_end_date`, `period_type`), merging non-empty cells |
| `macro_indicators` | `observation_date` | `terminal_macro_indicators` on `observation_date`, merging non-empty cells |
| `foreign_flows` | `ticker`, `trade_date`, `foreign_net_flow_kes` | `foreign_net_flow_kes` of existing `terminal_daily_prices` rows only |

Headers are lowercased and trimmed, with spaces replaced by underscores. Thousands separators are
stripped, parentheses mean negative, an empty cell is null, and dates are `YYYY-MM-DD` or
`DD/MM/YYYY`. Each import is recorded as `csv_import:<kind>`.

Every run is recorded in `terminal_ingestion_runs` and ends with one of these statuses:

| Status | Meaning | Data written | Exit code |
|---|---|---|---|
| `succeeded` | All rows valid | Yes | 0 |
| `partial` | Up to 10% of rows failed validation | Valid rows only | 0 |
| `no_new_data` | The source has not advanced | No | 0 |
| `layout_changed` | Headers, tables, links or row counts no longer match, or over 10% of rows are invalid | No | 3 |
| `blocked_by_robots` | `robots.txt` disallows the URL | No | 4 |
| `ambiguous_session` | The NSE session shown could not be dated; re-run with `--trade-date` | No | 5 |
| `failed` | Network failure after retries, lock held, exchange calendar not verified for the year, or an unexpected error | No (rolled back) | 1 |

Exit code 2 means invalid arguments, or from `seed` a schema not at head; 130 means interrupted.

**Scheduler.** `opennse-terminal scheduler` runs APScheduler in its own process, in
`TERMINAL_TIMEZONE` (Africa/Nairobi, UTC+3, no daylight saving):

| Job | Trigger | Pipelines |
|---|---|---|
| `nse_equities_eod` | Tuesday–Saturday 07:30, before the 09:30 open | `nse_market_statistics`, collecting the previous session |
| `cbk_macro` | Monday–Friday 18:15 | The five CBK pipelines in sequence |

Jobs use `coalesce=True`, `max_instances=1` and `misfire_grace_time=3600`, in an in-memory job
store, so a fire time missed while the process is down is not caught up. Each run holds a
transaction-scoped PostgreSQL advisory lock, so a scheduled run and an overlapping CLI run of the
same pipeline never both proceed: the later one ends `failed` with "lock held".

**Exchange calendar.** NSE snapshots are dated against `ExchangeCalendar`: the Public Holidays Act
rules plus `backend/opennse_terminal/ingestion/data/nse_closures.csv`, which lists gazetted closures
the rules cannot compute and is verified for 2026–2027. A run in any other year ends `failed`
(`CalendarNotVerified`) until that year's closures are added and its `verified_years` header line
extended, a data-only change. A run writes automatically only when the date of the page's daily
price-list link equals the expected session (the latest exchange session before the run) and, where
real closes of the session before it are stored, the reported price changes agree with them;
otherwise it ends `ambiguous_session`.

**Write precedence.**

- Price columns of `terminal_daily_prices` carry a row-level `source`, with precedence
  `csv_import` > `nse_market_statistics` > `seed`.
- `foreign_net_flow_kes` has its own provenance column, `flow_source`. Per-ticker foreign flow is not
  on the public NSE page, so it comes from the `foreign_flows` import (or the seed), and a scrape
  never overwrites it.
- `terminal_macro_indicators` and `terminal_fundamentals` record per-column provenance in
  `source_by_field`. CBK runs, PDF commits and imports overwrite only the columns they write.

## Configuration

Settings are read from `TERMINAL_*` environment variables or `.env` (copied from `.env.example`);
a process environment variable wins over the file.

| Variable | Default | Meaning |
|---|---|---|
| `TERMINAL_DATABASE_URL` | `postgresql+asyncpg://opennse:opennse@localhost:5432/opennse` | Async database URL (asyncpg driver required), used by the API, migrations, the seeder, the CLI and the scheduler |
| `TERMINAL_TEST_DATABASE_URL` | `postgresql+asyncpg://opennse:opennse@localhost:5432/opennse_test` | Administrative database of the integration suite; the role needs `CREATEDB` |
| `TERMINAL_DEFAULT_OWNER_KEY` | `local` | Subject of the default principal and owner of the seeded watchlist |
| `TERMINAL_TIMEZONE` | `Africa/Nairobi` | Exchange calendar and scheduler timezone (IANA name) |
| `TERMINAL_FUNDAMENTALS_LAG_DAYS` | `90` | Days after `period_end_date` before a fundamentals row is treated as published |
| `TERMINAL_SCRAPER_CONTACT` | `ops@example.invalid` | Contact address in the scrapers' user agent; set a real address, `local@domain` in visible ASCII with no spaces or control characters |
| `TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS` | `2.0` | Minimum spacing between requests to the same host |
| `TERMINAL_SCRAPER_TIMEOUT_SECONDS` | `30` | Read timeout per request (the connect timeout is fixed at 10 s) |
| `TERMINAL_SCRAPER_MAX_RETRIES` | `3` | Retries on timeouts and HTTP 429, 500, 502, 503 and 504 |
| `TERMINAL_PDF_MAX_BYTES` | `20971520` | Per-file PDF upload limit (20 MiB), at most 1073741824 (1 GiB); the whole request may carry 1 MiB more |
| `TERMINAL_PDF_MAX_PAGES` | `400` | Maximum pages per uploaded PDF |
| `TERMINAL_PDF_MAX_CONCURRENCY` | `2` | Concurrent PDF parses per API process, 1-32; further requests get 429 |
| `TERMINAL_API_ORIGIN` | `http://localhost:8000` | FastAPI origin of the Next.js rewrites and the Playwright servers; fixed at `next build` |

`TERMINAL_TEST_DATABASE_URL` and `TERMINAL_API_ORIGIN` are not backend settings and are ignored by
`TerminalSettings`. No secrets are required, because every source is public; `opennse`/`opennse`
are the development container's throwaway credentials. `.env` is git-ignored; never commit it.


## Data sources and compliance

All market and macro data comes from public sources: the NSE's public market-statistics web page,
CBK statistical releases, PDF financial statements and CSV/Excel files.

| Pipeline | Public source | Format and tools | Cadence | Writes |
|---|---|---|---|---|
| `nse_market_statistics` | NSE Market Statistics, "Equity Statistics" tab: `https://www.nse.co.ke/market-statistics/` | Table rendered by JavaScript, one sector at a time, about 70 rows (Playwright + BeautifulSoup4) | Daily | `terminal_daily_prices`: close and volume, joined to `terminal_equities` by ISIN. The page has no OHLC or turnover, so OHLC stays null and turnover is estimated as volume × close with `turnover_estimated = true` |
| `cbk_rates_csv` | CBK "Central Bank Rates" CSV, linked from `https://www.centralbank.go.ke/statistics/interest-rates/` | CSV (httpx + BeautifulSoup4 + pandas) | Monthly | Interbank rate, 91/182/364-day T-bills, Central Bank Rate |
| `cbk_inflation` | CBK inflation rates: `https://www.centralbank.go.ke/inflation-rates/` | HTML table (httpx + BeautifulSoup4) | Monthly | `inflation_yoy_pct` |
| `cbk_fx` | CBK "Exchange Rates End Period" CSV, linked from `https://www.centralbank.go.ke/statistics/exchange-rates/` | CSV (httpx + BeautifulSoup4 + pandas) | Monthly | `fx_kes_usd` |
| `cbk_bonds` | CBK Treasury bonds: `https://www.centralbank.go.ke/bills-bonds/treasury-bonds/` | HTML table (httpx + BeautifulSoup4) | Per auction | `terminal_bond_yields`; the yield curve uses fixed-coupon `FXD` bonds only |
| `cbk_reserves` | CBK Weekly Bulletin PDFs, listed at `https://www.centralbank.go.ke/releases/weekly-bulletin/` | PDF text layer (httpx + pdfplumber) | Weekly | `fx_reserves_usd_m`, each value dated by its own column header date |
| `POST /ingest/pdf` | Uploaded annual and interim reports and IPO prospectuses | PDF text layer (pdfplumber) | On demand | `terminal_corporate_filings`; on commit, `terminal_fundamentals` |
| `opennse-terminal import` | Operator-supplied files | CSV or Excel (pandas, openpyxl) | On demand | Per kind, as listed under [CLI and scheduling](#cli-and-scheduling) |

Compliance:

- **NSE terms.** The NSE's terms (`https://www.nse.co.ke/terms-and-conditions/`) vest the data's
  intellectual property in the NSE and permit personal or internal business use only. Operators who
  redistribute scraped data are responsible for obtaining a licence to do so.
- **CBK attribution.** CBK data is attributed "Source: Central Bank of Kenya", as its files state.
- **Polite scraping.** Scrapers identify themselves as
  `OpenNSE-Terminal/<version> (+mailto:<TERMINAL_SCRAPER_CONTACT>)`, check `robots.txt` before every
  URL, space requests at least 2 s apart per host, retry with exponential backoff that honours
  `Retry-After`, follow at most 5 redirects, abort responses above 25 MB, and keep no cookies or
  browser state between runs.

Known limitations:

- A source layout change ends the run `layout_changed` with nothing written; the scraper is not
  repaired automatically.
- There is no OCR: an image-only PDF has no text layer and returns `no_items` with the warning
  `no_text_layer`.
- There is no FX conversion: a non-KES statement is extracted in its own currency with the warning
  `non_kes_currency` and cannot be committed.
- Price history is not adjusted for corporate actions such as splits and bonus issues.
- A fundamentals row is treated as available 90 days after its period end. The lag reduces
  look-ahead bias in backtests and bands but does not remove it.
- The live NSE pipeline is verified for 2026 and 2027 only (see the exchange calendar above).

## Seed data provenance

The fallback seeder (`backend/opennse_terminal/seed/seed_terminal_db.py`) fills every table with
deterministic data, so every view renders before any scraper runs. Real data supersedes seed data
key by key: scraped and imported prices replace seeded prices, and CBK runs, PDF commits and imports
replace seeded macro and fundamentals values column by column. The seeder never updates a row
another writer has written.

| Table | From the seed specification | Filled from public observation | Synthetic |
|---|---|---|---|
| `terminal_equities` (8) | Ticker, company name, sector and shares outstanding of SCOM, EQTY, KCB, EABL, KEGN, COOP, ABSA and SBIC | ISINs (NSE market-statistics page) | Free float % |
| `terminal_daily_prices` (30,096) | None | 2026-09-30 closes and base daily volumes (NSE market-statistics page) | Everything else: 3,762 sessions of a synthetic weekday calendar from 2012-01-02 to 2026-09-30, price paths with election-cycle shapes, volumes, turnover, foreign flows (full coverage), and three planted volume spikes (SCOM 2026-09-15, KCB 2026-09-24, EABL 2026-09-29) |
| `terminal_fundamentals` (131 FY rows) | Share counts | None | Every line item, derived from target P/E, P/B, margins and payout; banks have no EBITDA, debt, cash or operating cash flow |
| `terminal_macro_indicators` (177 month-ends) | 2026-09-30: inflation 6.80, 91/182/364-day T-bills 15.72/16.58/16.84, interbank 12.85, KES/USD 129.20, reserves USD 7,850.00 m; 2026-08-31 inflation 6.60 | Central Bank Rate 8.75 on 2026-09-30 (CBK's published rate) | Every earlier month from 2012-01-31 |
| `terminal_bond_yields` (1,062) | None | None | Month-end `FXD-SEED-<tenor>Y-<YYYYMM>` yields for 2, 5, 10, 15, 20 and 25 years |
| `terminal_cycles` (4) | "2013 Cycle" 2013-03-04, "2017 Cycle" 2017-08-08, "2022 Cycle" 2022-08-09, "2027 Election Watch" 2027-08-10 | None | `event_type = 'general_election'` |
| `terminal_ipo_scenarios` (1) | Quickmart Supermarket, Retail & Consumer Goods, offer KES 7.50, 50.0% stake sold (secondary), primary proceeds 0.00, three risk scenarios | None | Share counts (1,000,000,000 outstanding, 500,000,000 offered) and every operating and valuation assumption |
| `terminal_watchlists` and items | None | None | "NSE Core 8", holding all eight tickers, owned by `TERMINAL_DEFAULT_OWNER_KEY` (`local`) |

- **Benchmarks differ from CBK.** The seed specification's September 2026 macro benchmarks differ
  from CBK's published figures. The seeder follows the specification verbatim; the first
  `cbk_rates_csv`, `cbk_inflation` and `cbk_fx` runs overwrite those columns for 2026-09-30, and
  `source_by_field` records which source supplied each column.
- **No future rows.** Nothing is seeded after 2026-09-30. The 2027 cycle's trajectory is returned by
  the backtester as a labelled projection (`is_projection = true`): the mean of the completed
  cycles' normalized paths, never stored.
- **Marked in code.** Every synthetic value in `backend/opennse_terminal/seed/reference_data.py`
  sits between `# --- SYNTHETIC --- <reason>` and `# --- END SYNTHETIC ---`; values taken from a
  public observation sit between `# --- FILLED (source: ...) ---` and `# --- END FILLED ---`.
- **Recognizable in the database.** Seeded rows carry `source = 'seed'` (prices also
  `flow_source = 'seed'`), and `source_by_field` maps every seeded macro and fundamentals column to
  `"seed"`.

To remove the seed once real data exists:

```sql
-- Row-level provenance.
DELETE FROM terminal_daily_prices WHERE source = 'seed';
UPDATE terminal_daily_prices SET foreign_net_flow_kes = NULL, flow_source = NULL
    WHERE flow_source = 'seed';
DELETE FROM terminal_bond_yields WHERE source = 'seed';
DELETE FROM terminal_cycles WHERE source = 'seed';
DELETE FROM terminal_ipo_scenarios WHERE source = 'seed';

-- Per-column provenance: repeat for each column of terminal_macro_indicators and
-- terminal_fundamentals.
UPDATE terminal_macro_indicators
    SET tbill_91d_pct = NULL, source_by_field = source_by_field - 'tbill_91d_pct'
    WHERE source_by_field ->> 'tbill_91d_pct' = 'seed';
UPDATE terminal_fundamentals
    SET net_income_kes = NULL, source_by_field = source_by_field - 'net_income_kes'
    WHERE source_by_field ->> 'net_income_kes' = 'seed';

-- Equities last, and only those nothing references: deleting an equity cascades to its prices,
-- fundamentals and watchlist items.
DELETE FROM terminal_equities e WHERE e.source = 'seed'
    AND NOT EXISTS (SELECT 1 FROM terminal_daily_prices p WHERE p.ticker = e.ticker)
    AND NOT EXISTS (SELECT 1 FROM terminal_fundamentals f WHERE f.ticker = e.ticker)
    AND NOT EXISTS (SELECT 1 FROM terminal_watchlist_items w WHERE w.ticker = e.ticker)
    AND NOT EXISTS (SELECT 1 FROM terminal_corporate_filings c WHERE c.ticker = e.ticker);
```

The seeded fundamentals rows (the per-column updates clear their values but keep the rows) and the
"NSE Core 8" watchlist reference all eight seeded tickers, so the last statement keeps those
equities until those rows are removed too.

