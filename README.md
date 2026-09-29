# Tally-Connect — TallyScrapper Middleware

A high-performance desktop middleware agent that synchronizes accounting, inventory, and order data from **Tally ERP 9 / Tally Prime** (via its XML HTTP interface) into a **PostgreSQL** database. 

Engineered specifically to solve Tally Prime freezes, out-of-memory crashes (`memory excess violation`), and inaccurate record counts through bounded streaming, intelligent request throttling, and exact database transaction tracking.

---

## Table of Contents

- [Key Highlights & Fixes](#key-highlights--fixes)
- [Architecture & Resilience](#architecture--resilience)
- [Quick Start](#quick-start)
- [Main Menu & Interactive Navigation](#main-menu--interactive-navigation)
- [Live Pause & Dynamic Reconfiguration (`P` Key)](#live-pause--dynamic-reconfiguration-p-key)
- [Performance Tuning & Window Settings](#performance-tuning--window-settings)
- [Selective Sync (Custom Period)](#selective-sync-custom-period)
- [Crash Prevention & Recovery](#crash-prevention--recovery)
- [Database Schema & Data Model](#database-schema--data-model)
- [Count Integrity & Voucher Classification](#count-integrity--voucher-classification)
- [Environment Variables](#environment-variables)
- [Diagnostics, Logs & Crash Recovery](#diagnostics-logs--crash-recovery)
- [Building Standalone Windows Executable](#building-standalone-windows-executable)
- [Unit Testing & Verification](#unit-testing--verification)

---

## Key Highlights & Fixes

* **Crash & Freeze Prevention:** Tally Prime answers XML requests on its main UI thread and can crash with a `memory excess violation` if requests overlap or query oversized date spans. The agent enforces a strict single-flight mutex lock (`withTallyThrottle`), adaptive cooldowns, and automatic request pacing.
* **Bounded Streaming XML Parser:** Employs low-memory streaming (`sax`) with Node.js backpressure (`stream.pause()` and `stream.resume()`). Memory usage remains flat (< 100 MB RSS) even when parsing multi-gigabyte Tally responses.
* **Accurate "New Records" Tracking:** Utilizes PostgreSQL's internal transaction metadata `(xmax = 0)` on `ON CONFLICT DO UPDATE` queries to strictly count genuinely new inserts versus records that were only updated or refreshed.
* **Contiguous Resumable Checkpoints:** Checkpoints advance strictly when past windows succeed. If a window encounters an issue, the checkpoint is held back, ensuring gaps are never skipped in subsequent runs.
* **Automated Failure Isolation:** If a database batch contains a malformed row, the agent uses a binary-search split (`oneBatchSafe`) to isolate and log the single offending record into `failures-*.log` while saving all remaining valid records.
* **Live Pause & Reconfiguration:** Pressing `P` at any time gracefully finishes the current in-flight window and opens an interactive menu to switch companies, change data types, or modify sync intervals without stopping the process.
* **Encrypted Machine-Local Storage:** Authentication sessions and database credentials are encrypted at rest using AES-256-GCM keyed to machine-specific hardware identifiers (`scryptSync`).

---

## Architecture & Resilience

```
┌─────────────────────────────────────────────────────────────┐
│                    Interactive CLI Engine                   │
│   • Auth Verification (Licentic)   • DB Connection Wizard   │
│   • Main Menu / Selective Settings • Live [P] Pause Monitor │
└──────────────────────────────┬──────────────────────────────┘
                               │
       ┌───────────────────────┴────────────────────────┐
       ▼                                                ▼
┌───────────────────────────┐                ┌───────────────────────────┐
│     Tally HTTP Client     │                │     PostgreSQL Engine     │
│ • withTallyThrottle lock  │                │ • Pool Management (pg)    │
│ • maxSockets: 1           │                │ • Auto DDL & Migrations   │
│ • Adaptive Cooldown & Gap │                │ • Savepoint Retries       │
│ • Character Sanitization  │                │ • Binary Batch Splitting  │
└──────────────┬────────────┘                │ • (xmax = 0) Insert Stats │
               │                             └─────────────▲─────────────┘
       ┌───────┴────────────────────┐                      │
       ▼                            ▼                      │
┌───────────────────────────┐┌───────────────────────────┐ │
│ SAX Stream Parsers        ││ xml2js DOM Parsers        │ │
│ (Memory-safe streaming)   ││ (Compact payloads)        │ │
│ • Vouchers (saxVoucher)   ││ • Companies               │ │
│ • Bills (saxBillsStream)  ││ • Ledgers                 │ │
│                           ││ • Orders & Inventory      │ │
└──────────────┬────────────┘└─────────────┬─────────────┘ │
               │                           │               │
               └─────────────┬─────────────┘               │
                             ▼                             │
┌────────────────────────────────────────────────────────┐ │
│                     Sync Handlers                      ├─┘
│  • Companies: Name regex + TDL start date discovery    │
│  • Ledgers: Balances, groups, phone/email/PAN parser   │
│  • Vouchers & Invoices: Windowed Register + Drafts     │
│  • Bills (Rec/Pay): Annual windows + snapshot replace  │
│  • Orders: TDL formula filtered Sales & Purchase       │
│  • Inventory: Stock items, groups, units, quantities   │
└────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- [Node.js](https://nodejs.org) (v18 or higher recommended)
- PostgreSQL database (local or remote)
- Tally ERP 9 or Tally Prime running with ODBC/HTTP Server enabled (default port `9000`)

### Installation & Run

```bash
# 1. Install dependencies
npm install

# 2. Start the middleware
npm start
```

### First-Time Setup
1. **License Login:** Enter your registered administrator email and password. Credentials authenticate against the license server and are saved encrypted in `~/tally-agent-login.enc`.
2. **Database Wizard:** Configure connection parameters (Local host/port/database or an external `postgres://` connection string). The agent tests connectivity and auto-creates all required tables and indexes.
3. **Main Menu:** Initiate sync, tune performance, or adjust Tally HTTP ports.

---

## Main Menu & Interactive Navigation

The agent is fully interactive and provides `[B] Back` navigation on every wizard screen:

```
================= TALLY AGENT =================
  Logged in as: admin@example.com
  [1] Start Sync
  [2] More Options (sync windows, batches, database, port)
  [3] Tally Port Settings (Change Tally HTTP Port)
  [4] Sync Settings (Selective Period [OFF])
  [5] Reset & Full Re-sync (clears all checkpoints)
  [6] Logout (delete the saved login from this computer)
  [7] Exit (keep the saved login)
=================================================
```

### Menu Options Explained

* **`[1] Start Sync`**: Launches the company discovery and sync workflow. Prompts for:
  - Company selection (`1,2` for specific companies, `0` for ALL).
  - Data types to synchronize (`0` for ALL, or comma-separated selections).
  - Sync interval (`0` for continuous sync with a 5-minute cooldown, or `N` minutes).
* **`[2] More Options`**: Submenu for performance tuning, switching PostgreSQL databases, and configuring Tally ports.
* **`[3] Tally Port Settings`**: Change the local Tally Prime port (persisted to `~/tally-agent-port.json`).
* **`[4] Sync Settings`**: Set a selective start year and month to avoid scanning full company history.
* **`[5] Reset & Full Re-sync`**: Deletes all saved checkpoints (`~/tally-agent-sync-state.json`), clears the `sync_state` table, and optionally wipes existing voucher and invoice records from PostgreSQL for a pristine re-fetch.
* **`[6] Logout`**: Clears memory credentials and deletes `~/tally-agent-login.enc`.
* **`[7] Exit`**: Closes the application while preserving your saved login and sync checkpoints.

---

## Live Pause & Dynamic Reconfiguration (`P` Key)

While an active sync or auto-sync loop is executing, you can press **`P`** on your keyboard at any time.

1. The agent catches the key event in real time.
2. It safely finishes writing the current date window to avoid partial database writes.
3. It displays the **Sync Paused Menu**:

```
========================================
⏸  SYNC PAUSED — CURRENT CONFIGURATION
========================================
   Companies : 1 selected — Acme Corp
   Data types: ALL
   Interval  : every 5 min
   Start from: April 2021 → today
   Windows   : 3 day(s) per Tally request
   Batches   : 500 record(s) per database write
----------------------------------------
  [1] Change companies (add / remove)
  [2] Change data types
  [3] Change interval
  [4] Resume syncing now
  [5] Stop & return to main menu
```

When resuming via `[4]`, the agent synchronizes immediately with the new configuration rather than waiting for the remaining interval timer.

---

## Performance Tuning & Window Settings

Accessible via Main Menu → `[2] More Options` → `[1] Sync Performance`. Persisted to `~/tally-agent-sync-tuning.json`.

| Setting | Default | Allowed Range | Recommended / Impact |
| :--- | :---: | :---: | :--- |
| **Sync Window Size** | `3` days | `1` to `31` days | Number of days requested from Tally per HTTP call. **1–3 days** is strongly recommended for large companies to prevent Tally out-of-memory errors. Values > 7 trigger a warning. |
| **Database Batch Size** | `500` | `100` to `2,000` | Number of records written per SQL `INSERT ... ON CONFLICT` statement. |
| **Recent Re-sync Window** | `30` days | `0` to `3,650` days | Number of recent days always re-scanned on every cycle, ensuring retroactive edits or newly back-dated vouchers are captured. Set to `0` to disable. |

---

## Selective Sync (Custom Period)

By default, the agent determines the starting date via:
1. `SYNC_START_YEAR` environment variable (if defined).
2. **Selective Sync Configuration** (`~/tally-agent-sync-config.json`).
3. Auto-detected company start date from Tally (e.g. `(From 1-Apr-2016)`).
4. Fallback: January 1, 1900.

Configuring Selective Sync via Main Menu `[4]` allows you to specify any start year and month (e.g., April 2022). The agent will only generate windows from that month forward, skipping unnecessary historical data and dramatically reducing initial sync time.

---

## Crash Prevention & Recovery

Tally Prime answers XML requests on a single UI thread and constructs responses entirely in its own memory. When requests are oversized or arrive too rapidly, Tally Prime crashes (`memory excess violation`) or freezes, leaving sync agents hanging indefinitely. Tally-Connect includes comprehensive crash prevention and recovery mechanisms:

### 1. Two-Phase Voucher Fetching
- **Phase 1 (Lightweight Header Scan):** Queries only essential metadata (`GUID`, `MasterID`, `AlterID`, `Date`, `VoucherTypeName`, `IsCancelled`, `IsOptional`). Response payloads are ~100x smaller, keeping Tally's memory usage minimal.
- **Diffing against PostgreSQL:** Compares header `AlterID` with `vouchers.alter_id` in the database.
- **Phase 2 (Targeted Details Fetch):** Heavy voucher details are requested **only for changed or new vouchers**. In steady state, up to 99% of windows stop after Phase 1, placing almost zero load on Tally.

### 2. Adaptive Window Runner & Bisection
- Every date window runs through `runAdaptive()`.
- If Tally fails or crashes, the runner prompts the user with clear restart instructions, waits for Tally and company reopen, and **halves the window** (e.g. 7d → 4d → 2d → 1d).
- The window controller adapts dynamically: windows shrink upon crash and gradually grow back by 1 day after 20 consecutive successful windows.

### 3. Voucher Isolation & Quarantine
- If a 1-day window crashes Tally repeatedly, the runner automatically switches to **voucher-by-voucher isolation** using TDL formula filters (`$MasterID = ...`).
- Poison or corrupt vouchers that trigger crashes are safely written to `~/tally-agent-quarantine.json` (`firstSeen`, `attempts`, `masterId`, `guid`, `reason`).
- All other healthy vouchers on that day are successfully saved, and sync execution proceeds without getting stuck.
- Quarantined vouchers are retried once per full re-scan cycle.
- Summary screens display quarantined counts with advice to run Tally Repair (`Alt+F3 → Data → Repair`) if corruption persists.

### 4. Heavy Entity Cooldowns & RAM Guard
- Automatic quiet cooldown (`TALLY_HEAVY_COOLDOWN_MS = 20000`) and health probes (`ensureTallyHealthy`) run before heavy entities (Bills, Vouchers).
- Continuous Windows working-set RAM guard monitors `tally*.exe`. If Tally reaches `TALLY_MEM_WARN_MB` (2500 MB), a warning is logged; if it exceeds `TALLY_MEM_MAX_MB` (3500 MB), the agent pauses and prompts for a clean restart before a fatal out-of-memory crash occurs.

### 5. Write-Ahead Request Logging
- Every outbound request is written to `~/tally-agent-logs/requests-YYYY-MM-DD.log` **before** sending it across the socket. If Tally crashes, the last log entry immediately identifies the exact request that caused the crash.

### 6. Reversible Kill Switch
- Set `TALLY_DETAIL_FETCH_MODE=legacy` to run the original single-phase code path unchanged.

---

## Database Schema & Data Model

All tables include a `payload JSONB` column that preserves the complete, unaltered Tally XML entity for complete data fidelity.

### 1. `companies`
Stores detected Tally companies.
- `company_guid` (TEXT, PK): Unique company identifier from Tally.
- `name` (TEXT): Company display name.
- `payload` (JSONB): Raw company metadata.
- `created_at`, `updated_at` (TIMESTAMPTZ).

### 2. `ledgers`
Stores chart of accounts, party ledgers, and contact information.
- `company_guid`, `ledger_guid` (TEXT, Composite PK).
- `name` (TEXT), `parent_group` (TEXT).
- `opening_balance`, `closing_balance` (NUMERIC).
- `type` (TEXT): `'Party'` or `'General'`.
- `phone` (TEXT): Extracted 10-digit mobile number.
- `email` (TEXT): Extracted email address.
- `pan` (TEXT): Income tax PAN number.
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 3. `vouchers`
Stores all accounting transactions (Sales, Purchase, Receipt, Payment, Journal, Contra, etc.).
- `company_guid`, `voucher_guid` (TEXT, Composite PK).
- `voucher_date` (DATE), `voucher_type` (TEXT), `reference_no` (TEXT).
- `net_amount` (NUMERIC): Max of debit/credit totals.
- `is_cancelled` (BOOLEAN): `TRUE` if `ISCANCELLED` or `ISDELETED` is set in Tally.
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 4. `invoices`
Extracted from commercial sales and purchase vouchers during the voucher sync.
- `company_guid`, `invoice_guid` (TEXT, Composite PK).
- `invoice_no` (TEXT), `invoice_date` (DATE), `invoice_type` (`'Sales'` / `'Purchase'`).
- `party_name` (TEXT), `total_amount` (NUMERIC).
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 5. `bills`
Stores outstanding Bills Receivable and Bills Payable snapshots.
- `id` (BIGSERIAL, PK).
- `company_guid` (TEXT), `source_hash` (TEXT, Unique Composite Key).
- `ledger_name` (TEXT), `bill_name` (TEXT), `bill_date` (DATE).
- `bill_amount` (NUMERIC), `pending_amount` (NUMERIC), `due_date` (DATE).
- `overdue_days` (INT), `bill_type` (`'RECEIVABLE'` / `'PAYABLE'`).
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 6. `orders`
Stores open Sales Orders and Purchase Orders.
- `company_guid`, `order_guid` (TEXT, Composite PK).
- `order_no` (TEXT), `order_date` (DATE), `party_name` (TEXT).
- `total_amount` (NUMERIC), `type` (`'Sales'` / `'Purchase'`).
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 7. `inventory_items`
Stores stock item masters, units, and valuations.
- `company_guid`, `item_guid` (TEXT, Composite PK).
- `name` (TEXT), `item_group` (TEXT), `unit` (TEXT).
- `opening_qty`, `opening_value`, `closing_qty`, `closing_value` (NUMERIC).
- `payload` (JSONB), `updated_at` (TIMESTAMPTZ).

### 8. `sync_state` & `sync_log`
- `sync_state`: Stores per-company, per-entity incremental metadata (checksums, last window processed, record counts).
- `sync_log`: Audit trail of sync execution records and timestamps.

---

## Count Integrity & Voucher Classification

The summary screen displays exact breakdowns matching Tally Prime's **Types of Vouchers** report:

```
========== COMPANY SUMMARY ==========
Company : Acme Enterprises Private Limited

Entity                     Fetched    New   Failed  Skipped    In DB
------------------------------------------------------------------------
Ledgers                        412      0        0        0      412
Bills Receivable                89      2        0        0       89
Bills Payable                   43      0        0        0       43
Vouchers                     1,840     15        0        0    1,840
Invoices                       920      8        0        0      920
Orders                          14      0        0        0       14
Inventory Items                256      0        0        0      256

🆕 NEW RECORDS FOUND
   • Bills Receivable: 2 new
   • Vouchers: 15 new
   • Invoices: 8 new

📊 Total New Records: 25

Voucher Type Breakdown (Active / Cancelled / Total):
  • Sales: 820 / 12 / 832
  • Purchase: 100 / 0 / 100
  • Receipt: 450 / 5 / 455
  • Payment: 410 / 3 / 413
  ── TOTAL: 1,780 / 20 / 1,800
```

### Reconciliation Logic
- **Active:** Confirmed (non-optional) vouchers that are not cancelled.
- **Cancelled:** All cancelled vouchers (including cancelled drafts).
- **Total:** Confirmed vouchers (`Active + Confirmed Cancelled`). Matches Tally Prime's on-screen total.
- **Draft / Optional Vouchers:** Vouchers marked `ISOPTIONAL = Yes` in Tally are stored in PostgreSQL but excluded from the standard total to maintain parity with Tally Prime's register. An explanatory note lists any excluded draft vouchers.

---

## Environment Variables

| Variable | Default | Purpose |
| :--- | :---: | :--- |
| `DEVELOPER_EMAIL` | *(None)* | Email for Developer Mode authentication. |
| `DEVELOPER_PASSWORD` | *(None)* | Password for Developer Mode authentication. |
| `TALLY_MIN_GAP_MS` | `3000` | Minimum quiet time (ms) between consecutive Tally HTTP requests. Real gap is adaptive based on request duration. |
| `TALLY_TIMEOUT_MS` | `180000` | Socket idle timeout (3 minutes) for Tally HTTP calls. Never infinite. |
| `TALLY_COOLDOWN_MS` | `30000` | Cooldown wait time (ms) applied when Tally experiences a transport error. |
| `TALLY_HEAVY_COOLDOWN_MS` | `20000` | Cooldown wait time (ms) before starting heavy entities (Bills, Vouchers). |
| `TALLY_DETAIL_FETCH_MODE` | `two-phase` | Voucher detail fetch mode: `two-phase` (light headers + targeted detail) or `legacy` (kill switch for original 1-phase). |
| `TALLY_MEM_WARN_MB` | `2500` | Tally process working-set RAM threshold (MB) for warning log. |
| `TALLY_MEM_MAX_MB` | `3500` | Tally process working-set RAM limit (MB) to pause and prompt user for restart. |
| `TALLY_MAX_WAIT_MIN` | `120` | Maximum wait time (minutes) for Tally to come back up after a restart. |
| `VOUCHER_STREAM_TIMEOUT_MS` | `600000` | Inactivity socket timeout (10 minutes) for streaming voucher responses. |
| `VOUCHER_WINDOW_DAYS` | `3` | Default window size in days (overridden by `sync-tuning.json`). Runner shrinks automatically on crash. |
| `RECENT_WINDOW_DAYS` | `30` | Default days for retroactive re-sync (overridden by `sync-tuning.json`). |
| `SYNC_START_YEAR` | *(None)* | Force a starting calendar year for historical sync. |
| `FULL_RESCAN_HOURS` | `24` | Triggers a full re-scan interval to heal any possible drift (`0` to disable). |
| `TALLY_FUTURE_HORIZON_DAYS` | `1825` | Maximum days ahead (5 years) to look for post-dated vouchers (fetched in 90-day header chunks). |
| `POSTDATED_REPORT_NAME` | *(Auto)* | Override TDL report name for post-dated vouchers. |
| `BACKEND_URL` | *(Render)* | Remote backend authentication service URL. |

---

## Diagnostics, Logs & Crash Recovery

### Log & State Files
All logs are stored in `~/tally-agent-logs/`:
- `requests-YYYY-MM-DD.log`: Write-ahead request log flushed **before** each outbound request, identifying the exact request that killed Tally after a crash.
- `sync-YYYY-MM-DD.log`: Structured JSON Lines logging every window, HTTP duration, payload byte size, memory RSS, and record counts.
- `failures-YYYY-MM-DD.log`: Detailed stack traces and JSON payloads of any isolated record that failed database insertion.
- `report.<date>.<time>.<pid>.json`: Automatic V8 diagnostic report generated upon fatal process exceptions or out-of-memory events.

Quarantine records:
- `~/tally-agent-quarantine.json`: Persistent record of isolated corrupted vouchers that cause Tally crashes, retried automatically on full re-scan cycles.

### Developer Mode
Run option `2` at the Authentication screen and supply `DEVELOPER_EMAIL` / `DEVELOPER_PASSWORD`. 
- Developer Mode reveals SQL statements, batch splits, XML heartbeat pulses, and low-level diagnostic logs directly in the terminal console.
- In normal mode, logs are written exclusively to disk while the terminal maintains a clean single-line live progress indicator (`\r`).

---

## Building Standalone Windows Executable

To compile a zero-dependency standalone `.exe`:

```bash
# Compile using pkg
npm run build
```

The resulting binary is written to `dist/Tally-Connect.exe`. It bundles Node.js 18 runtime, dependencies, and automatically allocates up to 4 GB of V8 heap space (`--max-old-space-size=4096`).

---

## Unit Testing & Verification

The repository includes a dedicated test suite verifying helper calculations, XML parsers, and SQL transactions:

```bash
# 1. Test date formatting, window calculation, and checkpoint logic
node test-helpers.cjs

# 2. Test SAX streaming parser across XML chunk boundaries
node test-bills-parser.cjs

# 3. Dry-run database SQL validation (requires active DB connection)
node test-db-sql.cjs
```
