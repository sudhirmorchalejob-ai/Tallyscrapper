require("dotenv").config({ quiet: true });

const axios = require("axios");
const Agent = require("agentkeepalive");
const xml2js = require("xml2js");
const sax = require("sax");
const { parseStringPromise } = xml2js;
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { StringDecoder } = require("string_decoder");
const crypto = require("crypto");
const readline = require("readline");
const { exec } = require("child_process");

const {
  CFG: TALLY_CFG,
  tallyRequest,
  withTallyThrottle: clientWithTallyThrottle,
  probeTally,
  tallyProcess,
  waitForTally,
  ensureTallyHealthy,
  sanitizeXml,
  logReq,
  TallyDownError,
  TallyLogicalError,
} = require("./tally-client");
const { createWindowController, runAdaptive, addDays, span } = require("./window-runner");
const { voucherHeadersXml, voucherDetailXml, voucherOneXml } = require("./voucher-requests");
const {
  ymd,
  parseHeaders,
  makeWork,
  makeIsolate,
  quarantineManager
} = require("./voucher-sync");

try {
  require("v8").setFlagsFromString("--max-old-space-size=4096");
} catch (e) {}

// CRASH DIAGNOSTICS. An out-of-memory or other fatal V8 error cannot be caught
// by any handler and closes the EXE window instantly, losing the reason. Node's
// built-in diagnostic report writes a JSON file (reason, stack, memory) at that
// moment, into the same folder as the sync logs.
try {
  if (process.report) {
    process.report.reportOnFatalError = true;
    process.report.reportOnUncaughtException = false;
    process.report.directory = require("path").join(require("os").homedir(), "tally-agent-logs");
    process.report.filename = ""; // default unique name: report.<date>.<time>.<pid>...json
    require("fs").mkdirSync(process.report.directory, { recursive: true });
  }
} catch {}

process.on("unhandledRejection", (err) => {
  try {
    structuredLog("error", "UNHANDLED REJECTION", {
      error: err && err.message ? err.message : String(err)
    });
  } catch {}
});

process.on("uncaughtException", (err) => {
  try {
    structuredLog("error", "UNCAUGHT EXCEPTION", {
      error: err && err.message ? err.message : String(err),
      stack: err && err.stack ? err.stack : null
    });
  } catch {}
});

const DEBUG = false;

// ============================================================
//  DEVELOPER MODE
//  Enabled ONLY via the "Developer Login" authentication option.
//  Controls log visibility (console output) exclusively — it never
//  bypasses license authentication, database authentication, Tally
//  connection, existing permissions or any sync logic.
// ============================================================
let isDeveloperMode = false;

// Technical/debug console output — printed only in Developer Mode.
// Structured file logging is unaffected and always written.
function devLog(...args) {
  if (isDeveloperMode) console.log(...args);
}

// ============================================================
//  LIVE PROGRESS — one terminal line, refreshed in place with \r
//  e.g.  "Fetching Vouchers... 500 done" -> "... 1000 done"
//  Normal mode only: Developer Mode keeps the full technical log
//  output (an inline line would collide with streaming logs).
//  Counts are real processed-record counts reported by the sync
//  flow itself — never estimated.
// ============================================================
const PROGRESS = { label: null, lastDrawn: -1, lastSuffix: "", status: null };

function progressSupported() {
  return !isDeveloperMode && process.stdout && process.stdout.isTTY;
}

function progressStart(label) {
  if (!progressSupported()) return;
  PROGRESS.label = label;
  PROGRESS.lastDrawn = -1;
  PROGRESS.lastSuffix = "";
  PROGRESS.status = null;
  PROGRESS.lastLen = 0;
  process.stdout.write(`${label}... 0 done`);
}

function progressDraw(n) {
  const parts = [PROGRESS.lastSuffix, PROGRESS.status].filter(Boolean);
  // Pad so a shorter line fully overwrites a longer previous one.
  const line = `\r${PROGRESS.label}... ${n.toLocaleString()} done${parts.length ? ` (${parts.join(" · ")})` : ""}`;
  process.stdout.write(line.padEnd(PROGRESS.lastLen || 0));
  PROGRESS.lastLen = line.length;
}

function progressUpdate(count, suffix = null) {
  if (!PROGRESS.label) return;
  const n = Number(count) || 0;
  const sfx = suffix === null ? PROGRESS.lastSuffix : suffix;
  // Redraw when either the count OR the suffix changed — the suffix keeps the
  // line alive (e.g. "97%") during long full re-syncs where old windows add
  // no new records and the count would otherwise stay frozen.
  if (n === PROGRESS.lastDrawn && sfx === PROGRESS.lastSuffix) return;
  PROGRESS.lastDrawn = n;
  PROGRESS.lastSuffix = sfx;
  // \r returns the cursor to the start of the same line so the previous
  // text is overwritten instead of printing thousands of new lines.
  progressDraw(n);
}

// Live status shown next to the percentage while one Tally request is open
// ("waiting for Tally 1m 5s" / "receiving 12.4 MB"), so a long wait is never
// mistaken for a frozen agent. Pass null to clear it.
function progressStatus(text) {
  if (!PROGRESS.label) return;
  if ((text || null) === PROGRESS.status) return;
  PROGRESS.status = text || null;
  progressDraw(Math.max(0, PROGRESS.lastDrawn));
}

// Finishes the inline line (newline) and reports completion. Keeps the last
// known suffix (e.g. "100%") so the final line reads "... 1,092 done (100%)".
function progressEnd(finalCount, entityLabel) {
  const hadLine = !!PROGRESS.label;
  const n = Number(finalCount) || 0;
  if (hadLine) {
    const label = PROGRESS.label;
    const sfx = PROGRESS.lastSuffix;
    PROGRESS.label = null;
    const line = `\r${label}... ${n.toLocaleString()} done${sfx ? ` (${sfx})` : ""}`;
    process.stdout.write(line.padEnd(PROGRESS.lastLen || 0) + "\n");
    PROGRESS.lastLen = 0;
    PROGRESS.status = null;
  }
  if (entityLabel && !isDeveloperMode) {
    console.log(`✅ ${entityLabel} sync completed`);
  }
}

// Terminates the current inline line WITHOUT ending progress, so warning
// or error messages print on their own clean line; the counter redraws
// automatically on the next update.
function progressBreak() {
  if (PROGRESS.label && PROGRESS.lastDrawn >= 0) {
    process.stdout.write("\n");
    PROGRESS.lastDrawn = -1;
  }
}

// ============================================================
//  HTTP AGENTS (Tally + long XML responses)
// Default socket idle timeout is 180000ms (3 minutes) — never infinite, so a hung
// Tally does not block the mutex forever. Override via TALLY_TIMEOUT_MS.
const TALLY_TIMEOUT_MS = Number(process.env.TALLY_TIMEOUT_MS) || 180000;
// Socket inactivity timeout must stay >= the axios request timeout so long Tally
// responses are never killed with "socket hang up" while still streaming.
const SOCKET_TIMEOUT = TALLY_TIMEOUT_MS;
const httpAgent = new Agent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 30000
});

const httpsAgent = new Agent.HttpsAgent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 30000
});

axios.defaults.httpAgent = httpAgent;
axios.defaults.httpsAgent = httpsAgent;
axios.defaults.timeout = TALLY_TIMEOUT_MS;

// Tally gets its OWN HTTP client instead of the shared pool above.
//   maxSockets: 1     — Tally Prime serves one report at a time; a pool of
//                       sockets just makes it open several at once.
//   maxFreeSockets: 0 — sockets are closed after every request instead of
//                       being kept alive, so Tally is never left holding an
//                       open connection between windows.
// The shared `axios` default stays for the remote license server, where a
// larger pool is harmless.
const tallyHttpAgent = new Agent({
  maxSockets: 1,
  maxFreeSockets: 0,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 0
});
const tallyHttpsAgent = new Agent.HttpsAgent({
  maxSockets: 1,
  maxFreeSockets: 0,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 0
});
const tallyHttp = axios.create({
  httpAgent: tallyHttpAgent,
  httpsAgent: tallyHttpsAgent,
  timeout: TALLY_TIMEOUT_MS,
  maxRedirects: 0
});

// Tally Prime HTTP endpoint configuration (dynamic port)
const PORT_CONFIG_FILE = path.join(os.homedir(), "tally-agent-port.json");
let TALLY_PORT = 9000;
let TALLY_URL = `http://localhost:${TALLY_PORT}`;

function loadTallyPort() {
  try {
    if (fs.existsSync(PORT_CONFIG_FILE)) {
      const data = JSON.parse(fs.readFileSync(PORT_CONFIG_FILE, "utf8"));
      if (data && data.port) {
        TALLY_PORT = Number(data.port) || 9000;
        TALLY_URL = `http://localhost:${TALLY_PORT}`;
      }
    }
  } catch {
    TALLY_PORT = 9000;
    TALLY_URL = `http://localhost:9000`;
  }
}

function saveTallyPort(port) {
  try {
    TALLY_PORT = Number(port) || 9000;
    TALLY_URL = `http://localhost:${TALLY_PORT}`;
    fs.writeFileSync(PORT_CONFIG_FILE, JSON.stringify({ port: TALLY_PORT }, null, 2));
  } catch (err) {
    console.error("⚠️ Failed to save Tally port configuration:", err.message);
  }
}

loadTallyPort();

// ============================================================
//  KEEP TALLY PRIME ALIVE — REQUEST THROTTLE
//
//  Tally Prime answers its XML/HTTP interface from the SAME single
//  process that draws the user interface, and it serves exactly ONE
//  request at a time: every "Export Data" request makes it build a
//  report before it can reply. When two requests overlap — or a
//  request is re-sent while Tally is still working on the previous
//  one — Tally stacks reports in memory instead of queueing them.
//  The symptom is Tally Prime going unresponsive ("Not Responding"),
//  and on a large company it dies outright with "memory excess
//  violation".
//
//  Every Tally request therefore goes through withTallyThrottle():
//    1. a strict single-flight lock (never two requests at once)
//    2. a minimum quiet gap measured from when the previous request
//       FINISHED, not from when it started
//    3. an adaptive gap + cooldown that widens when Tally looks
//       unhealthy and relaxes back to the base gap once it answers
//
//  Configure with TALLY_MIN_GAP_MS (default 1500) and
//  TALLY_COOLDOWN_MS (default 30000).
const TALLY_BASE_GAP_MS = Math.max(0, Number(process.env.TALLY_MIN_GAP_MS) || 3000);
const TALLY_MAX_GAP_MS = 60000;
const TALLY_COOLDOWN_MS = Math.max(0, Number(process.env.TALLY_COOLDOWN_MS) || 30000);
const TALLY_HEAVY_COOLDOWN_MS = Math.max(0, Number(process.env.TALLY_HEAVY_COOLDOWN_MS) || 20000);
const TALLY_DETAIL_FETCH_MODE = process.env.TALLY_DETAIL_FETCH_MODE || "two-phase";

let tallyGapMs = TALLY_BASE_GAP_MS;
let tallyChain = Promise.resolve();
let tallyLastDoneAt = 0;
let tallyCooldownUntil = 0;
let tallyInFlight = 0;
let tallyRequestCount = 0;

// A clean LINEERROR / empty report means Tally answered normally — that is NOT
// a health problem, so it must not buy cooldown time. Only transport failures
// (timeouts, resets, 5xx) mean Tally is struggling.
function isTallyUnhealthy(err) {
  if (!err) return false;
  if (err.isTallyTimeout) return true;
  const code = String(err.code || "");
  if (/^(ECONNRESET|ECONNREFUSED|ECONNABORTED|EPIPE|ETIMEDOUT|EHOSTUNREACH|ENETUNREACH)$/.test(code)) return true;
  const status = err.response?.status;
  if (typeof status === "number" && status >= 500) return true;
  return /socket hang up|ECONNRESET|read ETIMEDOUT|timeout of \d+ms exceeded/i.test(err.message || "");
}

// Quiet period after a failure. Tally keeps working on a request we already
// gave up on, so the next request must NOT be sent until it has finished.
// Repeated failures also widen the normal gap, so a struggling Tally is polled
// progressively more gently instead of being hammered.
function tallyThrottleBackoff(label, err) {
  const until = Date.now() + TALLY_COOLDOWN_MS;
  if (until > tallyCooldownUntil) tallyCooldownUntil = until;
  tallyGapMs = Math.min(TALLY_MAX_GAP_MS, Math.max(TALLY_BASE_GAP_MS, Math.round(tallyGapMs * 2)));
  structuredLog("warn", `[tally] THROTTLE: ${label} failed (${err?.message || err}) — no request for ${Math.round(TALLY_COOLDOWN_MS / 1000)}s, gap now ${tallyGapMs}ms`, {
    error: err?.message || String(err)
  });
}

// A request that completed cleanly means Tally is coping — relax the gap back
// towards the base pace.
function tallyThrottleRecover() {
  if (tallyGapMs > TALLY_BASE_GAP_MS) {
    tallyGapMs = Math.max(TALLY_BASE_GAP_MS, Math.round(tallyGapMs / 2));
  }
}

/**
 * Runs `fn` with exclusive access to Tally Prime, then holds the lock until
 * the request is completely finished and the quiet gap has elapsed.
 * @param {() => Promise<any>} fn - the request to run
 * @param {string} label - entity name used in the throttle log lines
 */
function withTallyThrottle(fn, label = "request") {
  return clientWithTallyThrottle(fn);
}

// Report names Tally Prime has answered with LINEERROR. Asking again only
// repeats a request the server already refused, so they are remembered for the
// lifetime of the process.
const TALLY_MISSING_REPORTS = new Set();

function getTallyThrottleState() {
  return {
    gapMs: tallyGapMs,
    cooldownRemainingMs: Math.max(0, tallyCooldownUntil - Date.now()),
    inFlight: tallyInFlight,
    requests: tallyRequestCount
  };
}

// ============================================================
//  TALLY HTTP REQUEST HELPER WITH THROTTLE, TIMEOUT & RETRIES
//
//  Every attempt goes through withTallyThrottle(), so Tally Prime
//  never has more than one report building at a time and always gets
//  a quiet gap before the next request. See the throttle section
//  above for why this matters.
// ============================================================
async function postTallyXml(xmlData, options = {}) {
  const retries = options.retries ?? 3;
  const timeout = options.timeout ?? TALLY_TIMEOUT_MS;
  const label = options.entity || "request";

  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const data = await tallyRequest(xmlData, {
        idleMs: timeout,
        kind: label,
        meta: { attempt }
      });
      return { data };
    } catch (err) {
      lastError = err;
      const isTimeout = err instanceof TallyDownError || (err.message && err.message.includes("timeout"));
      if (attempt < retries) {
        devLog(`  ⚠️ Tally XML request ${isTimeout ? "timed out" : "failed"} (Attempt ${attempt}/${retries}): ${err.message}. Retrying in 2s...`);
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
  }
  throw lastError;
}

// Helper to split date ranges into fixed-size chunks (prevents huge Tally responses & timeouts)
function fmtYMD(d) {
  const yr = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const dy = String(d.getDate()).padStart(2, "0");
  return `${yr}${mo}${dy}`;
}

// Fixed-size rolling date windows used for vouchers.
// Every window stays small so Tally never has to build a huge single response
// (the root cause of socket hang-ups / timeouts on large companies).
function getVoucherWindows(startDate, days = 5) {
  const now = new Date();
  const windows = [];
  const cur = new Date(startDate);
  let guard = 0;
  while (cur <= now && guard < 20000) {
    const from = new Date(cur);
    const to = new Date(cur);
    to.setDate(to.getDate() + days - 1);
    if (to > now) to.setTime(now.getTime());
    windows.push({
      fromDate: fmtYMD(from),
      toDate: fmtYMD(to),
      label: fmtYMD(from)
    });
    cur.setDate(cur.getDate() + days);
    guard++;
  }
  return windows;
}

// Annual date windows used for bills sync. Bills are few (one row per open
// invoice), so a single year per request keeps the report small without the
// request explosion of the 5-day voucher windows. `startMonth` defaults to 4
// (April = Indian financial year start) but honours a selective-sync month.
function getBillsWindows(startYear = null, startMonth = 4) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const fromYear = startYear || (currentYear - 1);

  // Normalize the month (1-12, 1 = Jan)
  let sm = parseInt(startMonth, 10);
  if (isNaN(sm) || sm < 1 || sm > 12) sm = 4;

  const windows = [];
  const cur = new Date(fromYear, sm - 1, 1); // start of FY
  let guard = 0;

  while (cur <= now && guard < 100) {
    const y = cur.getFullYear();
    const firstDay = new Date(y, sm - 1, 1);
    // Last day of the FY is the day before next FY's first day
    const lastDay = new Date(y + 1, sm - 1, 0); // end of same FY
    const fromDate = fmtYMD(firstDay);
    const toDate = lastDay > now ? fmtYMD(now) : fmtYMD(lastDay);

    windows.push({
      fromDate,
      toDate,
      label: fromDate
    });

    // Advance to next FY start
    cur.setFullYear(y + 1);
    guard++;
  }

  return windows;
}

// Default start of the full-history window when Tally does not report a company
// "Starting From" date. Overridable via SYNC_START_YEAR (e.g. 2005) or the
// selective sync config (year + month). Kept self-contained for unit tests.
function resolveSyncStartYear(company) {
  return resolveSyncPeriod(company).startYear;
}

let pool = null;

const DB_CONFIG_FILE = path.join(os.homedir(), "tally-agent-db-config.enc");

// Derive a machine-local encryption key (so the stored file is encrypted at rest)
function getEncryptionKey() {
  const seed = `${os.hostname()}|${os.platform()}|${os.arch()}|${os.userInfo().username}`;
  return crypto.scryptSync(seed, "tally-agent-config-v1", 32);
}

function encryptConfig(obj) {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const data = Buffer.from(JSON.stringify(obj), "utf8");
  const enc = Buffer.concat([cipher.update(data), cipher.final()]);
  const tag = cipher.getAuthTag();
  return JSON.stringify({
    v: 1,
    iv: iv.toString("base64"),
    tag: tag.toString("base64"),
    data: enc.toString("base64")
  });
}

function decryptConfig(raw) {
  const key = getEncryptionKey();
  const parsed = JSON.parse(raw);
  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    key,
    Buffer.from(parsed.iv, "base64")
  );
  decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
  const dec = Buffer.concat([
    decipher.update(Buffer.from(parsed.data, "base64")),
    decipher.final()
  ]);
  return JSON.parse(dec.toString("utf8"));
}

function saveDbConfig(cfg) {
  fs.writeFileSync(DB_CONFIG_FILE, encryptConfig(cfg));
}

function loadDbConfig() {
  if (!fs.existsSync(DB_CONFIG_FILE)) return null;
  try {
    return decryptConfig(fs.readFileSync(DB_CONFIG_FILE, "utf8"));
  } catch (err) {
    console.log("⚠️ Could not decrypt saved config. Starting wizard.");
    return null;
  }
}

function buildPoolConfig(cfg) {
  if (cfg.connectionString) {
    return {
      connectionString: cfg.connectionString,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
    };
  }
  return {
    host: cfg.host || "localhost",
    port: Number(cfg.port) || 5432,
    user: cfg.user,
    password: cfg.password,
    database: cfg.database,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  };
}

async function testDbConnection(cfg) {
  const testPool = new Pool(buildPoolConfig(cfg));
  try {
    await testPool.query("SELECT 1");
    return true;
  } catch (err) {
    return false;
  } finally {
    await testPool.end();
  }
}

async function connectWithConfig(cfg) {
  if (pool) {
    await pool.end().catch(() => {});
  }
  pool = new Pool(buildPoolConfig(cfg));
  await initDb();
}

/* --------------------------
   DATABASE CONFIG WIZARD
---------------------------- */
const BACK_SIGNAL = Symbol("BACK");

// Prompts for one wizard field; typing B throws BACK_SIGNAL so the wizard
// can step back to its previous screen.
async function askField(query, hideInput = false) {
  const answer = (await askQuestion(query, hideInput)).trim();
  if (answer.toLowerCase() === "b") throw BACK_SIGNAL;
  return answer;
}

async function dbWizard() {
  console.log("\n🛠️  Database Configuration Wizard");
  console.log("======================================");
  console.log("ℹ️ Type B at any prompt to go back.\n");

  while (true) {
    const modeInput = await askQuestion(
      "\nDatabase type — [1] Local PostgreSQL, [2] External PostgreSQL, [B] Back: "
    );
    const modeChoice = modeInput.trim();

    if (modeChoice.toLowerCase() === "b") {
      console.log("↩️ Back.");
      return null;
    }
    if (modeChoice !== "1" && modeChoice !== "2") {
      console.log("⚠️ Invalid option. Please choose 1, 2, or B.");
      continue;
    }

    let cfg;
    try {
      cfg = { mode: modeChoice === "2" ? "external" : "local" };

      if (modeChoice === "2") {
        const useUrl = (await askField("Use a connection URL? [y/N]: ")).toLowerCase();
        if (useUrl === "y") {
          cfg.connectionString = await askField(
            "Connection URL (postgres://user:pass@host:port/db): "
          );
        } else {
          cfg.host = await askField("Host: ");
          cfg.port = (await askField("Port [5432]: ")) || "5432";
          cfg.database = await askField("Database name: ");
          cfg.user = await askField("Username: ");
          cfg.password = await askField("Password: ", true);
        }
      } else {
        cfg.host = (await askField("Host [localhost]: ")) || "localhost";
        cfg.port = (await askField("Port [5432]: ")) || "5432";
        cfg.database = await askField("Database name: ");
        cfg.user = await askField("Username: ");
        cfg.password = await askField("Password: ", true);
      }
    } catch (err) {
      if (err === BACK_SIGNAL) {
        console.log("\n↩️ Back to database type selection.");
        continue;
      }
      throw err;
    }

    console.log("\n🔌 Testing connection...");
    if (await testDbConnection(cfg)) {
      console.log("✅ Connection successful!");
      saveDbConfig(cfg);
      console.log("🔐 Configuration saved securely (encrypted) for next launch.\n");
      return cfg;
    }

    console.log("❌ Connection failed. Please check the details and try again.\n");
  }
}

/* --------------------------
   AUTHENTICATION & BROWSER HELPERS
---------------------------- */
let AUTH_TOKEN = null;
let ADMIN_EMAIL = null;
let ADMIN_NAME = null;
const BACKEND_URL = process.env.BACKEND_URL || "https://tally-connect-yga1.onrender.com";

/**
 * Opens a URL in the operating system's default web browser.
 * Compatible with Node.js, pkg compiled executable, Windows, macOS, and Linux.
 * @param {string} url - The URL to open.
 * @returns {Promise<void>}
 */
function openDefaultBrowser(url) {
  return new Promise((resolve, reject) => {
    let command;
    if (process.platform === "win32") {
      command = `start "" "${url}"`;
    } else if (process.platform === "darwin") {
      command = `open "${url}"`;
    } else {
      command = `xdg-open "${url}"`;
    }

    exec(command, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Handles the Forgot Password flow by redirecting the user to the web portal.
 */
async function handleForgotPassword() {
  const resetUrl = "https://tally-connect.com/forgot-password";
  try {
    await openDefaultBrowser(resetUrl);
    console.log("\nOpening your default browser...\n");
    console.log("Redirecting to:\n");
    console.log(`${resetUrl}\n`);
    console.log("Please reset your password using the web portal.\n");
    console.log("After resetting your password, return to this terminal and log in again.\n");
  } catch (err) {
    console.log("\nUnable to open your browser automatically.\n");
    console.log("Please manually visit:\n");
    console.log(`${resetUrl}\n`);
  }
  await askQuestion("Press Enter to continue...");
}

/**
 * Displays the Authentication Menu.
 */
async function showAuthenticationMenu() {
  const saved = loadLoginSession();
  console.log("\n========================================");
  console.log("       Tally-Connect Authentication");
  console.log("========================================");
  console.log("");
  if (saved) {
    console.log(`ℹ️  A login is saved on this computer for: ${saved.email}`);
  }
  console.log("1. Login");
  console.log("2. Developer Login");
  console.log("3. Forgot Password");
  console.log("4. Exit");
  console.log("");
  const choice = (await askQuestion("Select an option: ")).trim();
  return choice;
}

const LICENSE_BASE_URL = "https://dashboard.licentic.org";
const LICENSE_PRODUCT_ID = "695902cfc240b17f16c3d716";

// ============================================================
//  SAVED LOGIN (encrypted, machine-local)
//  Stores the license JWT + admin details so the agent does not
//  ask for email/password on every restart. The file is encrypted
//  with the same AES-256-GCM machine-local key as the database
//  config, so it cannot be read on another machine/account.
//  Removed ONLY by an explicit "Logout" (main menu), never by
//  exiting the agent.
// ============================================================
const LOGIN_SESSION_FILE = path.join(os.homedir(), "tally-agent-login.enc");

function saveLoginSession(session) {
  try {
    fs.writeFileSync(
      LOGIN_SESSION_FILE,
      encryptConfig({ ...session, savedAt: new Date().toISOString() })
    );
    return true;
  } catch (err) {
    console.log(`⚠️ Could not save the login on this computer: ${err.message}`);
    return false;
  }
}

function loadLoginSession() {
  try {
    if (!fs.existsSync(LOGIN_SESSION_FILE)) return null;
    const parsed = decryptConfig(fs.readFileSync(LOGIN_SESSION_FILE, "utf8"));
    if (!parsed || !parsed.email || !parsed.token) return null;
    return parsed;
  } catch {
    return null;
  }
}

function clearLoginSession() {
  try {
    if (fs.existsSync(LOGIN_SESSION_FILE)) fs.unlinkSync(LOGIN_SESSION_FILE);
    return true;
  } catch (err) {
    console.log(`⚠️ Could not remove the saved login: ${err.message}`);
    return false;
  }
}

// Reads the expiry out of a JWT. The signature is NOT verified here — the token
// is only accepted after the license server has confirmed the license is still
// active (see fetchActiveLicense).
function getJwtExpiryMs(token) {
  try {
    const payload = JSON.parse(Buffer.from(String(token).split(".")[1], "base64").toString("utf8"));
    return payload.exp ? Number(payload.exp) * 1000 : null;
  } catch {
    return null;
  }
}

// Single source of truth for the license check, shared by the interactive login
// and by the saved-login restore. Returns { ok, reason, license, expiryInfo }.
async function fetchActiveLicense(email) {
  const licenseUrl = `${LICENSE_BASE_URL}/api/external/actve-license/${encodeURIComponent(email)}?productId=${LICENSE_PRODUCT_ID}`;
  const licenseRes = await axios.get(licenseUrl, { timeout: 15000 });
  const activeLicense = licenseRes.data?.activeLicense;
  const expiryInfo = licenseRes.data?.expiryInfo;

  if (!activeLicense || expiryInfo?.isExpired) {
    return {
      ok: false,
      reason: expiryInfo?.isExpired ? "expired" : "none",
      expiryInfo
    };
  }
  return { ok: true, license: activeLicense, expiryInfo };
}

function describeLicense(activeLicense, expiryInfo) {
  const planName = activeLicense.licenseTypeId?.name || "Standard";
  const licenseKey = activeLicense.licenseKey || "N/A";
  const endDateRaw = activeLicense.validUntil || activeLicense.endDate || expiryInfo?.endDate;
  const validUntil = endDateRaw ? new Date(endDateRaw).toLocaleDateString() : "N/A";
  return { planName, licenseKey, validUntil };
}

/**
 * Restores a previously saved login so the agent does not ask for credentials
 * again after a restart. The stored token must not be expired AND the license
 * must still be active; otherwise the stale file is deleted and the normal
 * login screen is shown. A network failure keeps the file (it may still be
 * valid once connectivity returns) and simply falls through to manual login.
 */
async function tryRestoreLogin() {
  const session = loadLoginSession();
  if (!session) return false;

  console.log("\n🔐 Restoring your saved login...");

  const expiresAt = getJwtExpiryMs(session.token);
  if (expiresAt && expiresAt <= Date.now()) {
    console.log("🔒 The saved login has expired. Please log in again.");
    clearLoginSession();
    return false;
  }

  try {
    const license = await fetchActiveLicense(session.email);
    if (!license.ok) {
      console.log(
        license.reason === "expired"
          ? "⚠️ Your license has expired. Please log in again."
          : "⚠️ No active license found for the saved account. Please log in again."
      );
      clearLoginSession();
      return false;
    }

    const { planName, licenseKey, validUntil } = describeLicense(license.license, license.expiryInfo);
    ADMIN_EMAIL = session.email;
    AUTH_TOKEN = session.token;
    if (session.name) ADMIN_NAME = session.name;

    console.log("\n✅ Welcome back — login restored from this computer.");
    console.log(`👤 Admin: ${session.name || session.email} (${session.email})`);
    console.log(`🔑 License Key: ${licenseKey}`);
    console.log(`📦 License Plan: ${planName}`);
    console.log(`📅 Valid Until: ${validUntil}\n`);
    return true;
  } catch (err) {
    console.log(`⚠️ Could not verify the saved login (${err.message}).`);
    console.log("Please log in to continue.\n");
    return false;
  }
}

/**
 * Logout — clears the in-memory session AND deletes the saved login file from
 * this computer. Exiting the agent (main menu [7]) never does this.
 * @returns {Promise<boolean>} true when the user actually logged out.
 */
async function logout() {
  console.log("\n🚪 Logout");
  console.log("======================================");
  console.log(`Currently logged in as: ${ADMIN_EMAIL || "nobody"}`);
  console.log("This deletes the saved login from this computer, so the next");
  console.log("start (and any user of this Windows account) must log in again.");
  console.log("Use [7] Exit instead if you only want to close the agent.\n");

  const answer = (await askQuestion("Delete the saved login and log out? (Y/N): ")).trim().toLowerCase();
  if (answer !== "y" && answer !== "yes") {
    console.log("↩️ Still logged in.\n");
    return false;
  }

  AUTH_TOKEN = null;
  ADMIN_EMAIL = null;
  ADMIN_NAME = null;
  const removed = clearLoginSession();
  console.log(removed ? "\n✅ Logged out. Saved login details deleted from this computer.\n"
                      : "\n✅ Logged out.\n");
  return true;
}

/**
 * Authenticates the user strictly against the License System using email & password,
 * then verifies the user's active license and receives JWT token.
 */
async function agentLogin() {
  console.log("\n🔐 License Authentication");
  console.log("========================================");
  const email = (await askQuestion("Enter admin email (B = back): ")).trim();

  if (email.toLowerCase() === "b") {
    console.log("\n↩️ Back to authentication menu.\n");
    return false;
  }

  const password = await askQuestion("Enter admin password: ", true);

  if (!email || !password) {
    console.log("⚠️ Email and password cannot be empty.");
    return false;
  }

  console.log("\n🔍 Authenticating credentials with License System...");

  try {
    // Step 1: Validate Email & Password against License System
    const loginRes = await axios.post(
      `${LICENSE_BASE_URL}/api/auth/login`,
      { email, password },
      { timeout: 15000 }
    );

    if (!loginRes.data?.success || !loginRes.data?.token) {
      console.log(`\n❌ License Authentication Failed: ${loginRes.data?.message || "Authentication failed."}`);
      return false;
    }

    const jwtToken = loginRes.data.token;
    const userObj = loginRes.data.user;

    console.log("🔍 Verifying active license...");

    // Step 2: Verify Active License
    const license = await fetchActiveLicense(email);

    if (!license.ok) {
      console.log(`\n❌ License Verification Failed: No active license found for '${email}'.`);
      if (license.reason === "expired") {
        console.log(`⚠️ Reason: License expired on ${license.expiryInfo?.endDate ? new Date(license.expiryInfo.endDate).toLocaleDateString() : "unknown date"}.`);
      }
      return false;
    }

    const { planName, licenseKey, validUntil } = describeLicense(license.license, license.expiryInfo);

    ADMIN_EMAIL = email;
    ADMIN_NAME = userObj?.name || email;
    AUTH_TOKEN = jwtToken;
    // Remember this login on this computer so restarting the agent does not
    // ask for the password again. Only an explicit Logout removes it.
    saveLoginSession({ email, token: jwtToken, name: userObj?.name || email });

    console.log("\n✅ License Authenticated Successfully!");
    console.log(`👤 Admin: ${ADMIN_NAME} (${email})`);
    console.log(`🔑 License Key: ${licenseKey}`);
    console.log(`📦 License Plan: ${planName}`);
    console.log(`📅 Valid Until: ${validUntil}`);
    console.log("💾 Login saved on this computer — you won't need to log in again.\n");
    return true;

  } catch (err) {
    if (err.response?.data?.message) {
      console.log(`\n❌ License Authentication Failed: ${err.response.data.message}`);
    } else if (err.response?.data?.errors && Array.isArray(err.response.data.errors)) {
      console.log(`\n❌ License Authentication Failed: ${err.response.data.errors.join(", ")}`);
    } else {
      console.log(`\n❌ License Authentication Failed: Could not connect to License System (${err.message}).`);
    }
    return false;
  }
}

/**
 * Developer Login — enables Developer Mode (log visibility only).
 * Does NOT authenticate the license flow and does NOT bypass any
 * existing authentication; the normal login is still required.
 * Credentials are provided via DEVELOPER_EMAIL and DEVELOPER_PASSWORD env variables.
 */
async function developerLogin() {
  console.log("\nDeveloper Login");
  console.log("========================================");
  const email = (await askQuestion("Enter developer email (B = back): ")).trim();

  if (email.toLowerCase() === "b") {
    console.log("\n↩️ Back to authentication menu.\n");
    return false;
  }

  const password = await askQuestion("Enter developer password: ", true);

  const devEmail = process.env.DEVELOPER_EMAIL;
  const devPassword = process.env.DEVELOPER_PASSWORD;

  if (email === devEmail && password === devPassword) {
    isDeveloperMode = true;
    console.log("\n✅ Developer authentication successful.");
    console.log("🔧 Developer Mode enabled.\n");
    return true;
  }

  console.log("\n❌ Invalid developer credentials.");
  return false;
}

/**
 * Runs the Authentication Flow loop until successful login or exit.
 * A login saved on this computer is restored automatically, so restarting
 * the agent after an Exit does not require the password again.
 */
async function startAuthenticationFlow() {
  if (await tryRestoreLogin()) return;

  let authenticated = false;

  while (!authenticated) {
    const choice = await showAuthenticationMenu();

    if (choice === "1") {
      const loginSuccess = await agentLogin();
      if (loginSuccess) {
        authenticated = true;
      }
    } else if (choice === "2") {
      await developerLogin();
    } else if (choice === "3") {
      await handleForgotPassword();
    } else if (choice === "4") {
      console.log("\n👋 Exiting TallyScrapper. Goodbye!");
      process.exit(0);
    } else {
      console.log("\n⚠️ Invalid option. Please select 1, 2, 3, or 4.");
    }
  }
}

/* --------------------------
   TALLY PORT CONFIG WIZARD
---------------------------- */
async function tallyPortWizard() {
  console.log("\n⚙️  Tally HTTP Port Configuration");
  console.log("======================================");
  console.log(`Current Tally Port: ${TALLY_PORT} (Endpoint: ${TALLY_URL})\n`);

  const input = await askQuestion(`Enter new Tally HTTP Port [${TALLY_PORT}] (B = back): `);
  const trimmed = input.trim();

  if (trimmed.toLowerCase() === "b") {
    console.log("↩️ Back to main menu.\n");
    return;
  }

  if (!trimmed) {
    console.log(`ℹ️ Tally Port kept as: ${TALLY_PORT}\n`);
    return;
  }

  const newPort = parseInt(trimmed, 10);
  if (isNaN(newPort) || newPort < 1 || newPort > 65535) {
    console.log("❌ Invalid port number. Port must be between 1 and 65535.\n");
    return;
  }

  saveTallyPort(newPort);
  console.log(`✅ Tally HTTP Port updated successfully to: ${TALLY_PORT}`);
  console.log(`📡 New Tally Endpoint: ${TALLY_URL}\n`);
}

/* --------------------------
   MAIN MENU
---------------------------- */
async function mainMenu() {
  const syncCfg = selectiveSyncConfig;
  const syncLabel = syncCfg.enabled
    ? ` [ON: ${MONTH_NAMES[syncCfg.startMonth - 1]} ${syncCfg.startYear}]`
    : " [OFF]";

  console.log("\n================= TALLY AGENT =================");
  if (ADMIN_EMAIL) {
    console.log(`  Logged in as: ${ADMIN_EMAIL}`);
  }
  console.log("  [1] Start Sync");
  console.log("  [2] More Options (sync windows, batches, database, port)");
  console.log("  [3] Tally Port Settings (Change Tally HTTP Port)");
  console.log(`  [4] Sync Settings (Selective Period${syncLabel})`);
  console.log("  [5] Reset & Full Re-sync (clears all checkpoints)");
  console.log("  [6] Logout (delete the saved login from this computer)");
  console.log("  [7] Exit (keep the saved login)");
  console.log("=================================================");

  const choice = (await askQuestion("\nSelect an option: ")).trim();
  return choice;
}

/* --------------------------
   MORE OPTIONS (main menu [2])
   Groups the settings that are not part of a normal sync run so the
   main menu stays short.
---------------------------- */
async function moreOptionsMenu() {
  const t = syncTuning;
  console.log("\n⚙️  More Options");
  console.log("======================================");
  console.log(`  Sync windows  : ${t.windowDays} day(s) per Tally request`);
  console.log(`  Batch size    : ${t.batchSize} record(s) per database write`);
  console.log(`  Recent re-sync: last ${t.recentWindowDays} day(s) always re-fetched`);
  console.log("--------------------------------------");
  console.log("  [1] Sync Performance (windows / batches / recent days)");
  console.log("  [2] Database Settings (Change Database Configuration)");
  console.log("  [3] Tally Port Settings (Change Tally HTTP Port)");
  console.log("  [B] Back to main menu");
  console.log("======================================");

  return (await askQuestion("\nSelect an option: ")).trim();
}

/* --------------------------
   SYNC PERFORMANCE
   How many days one Tally request covers and how many records are
   written per database batch. Saved to disk, so it survives restarts.
---------------------------- */
async function configureSyncPerformance() {
  console.log("\n⚡ Sync Performance");
  console.log("======================================");
  console.log("  Sync window (days)  — days of data asked from Tally per request.");
  console.log(`      Fewer days = smaller Tally responses, more requests, more detailed`);
  console.log(`      resume checkpoints.  Current: ${syncTuning.windowDays} day(s).`);
  console.log("  Batch size (records) — records written to the database per batch/transaction.");
  console.log(`      More records = faster on light data, more memory per batch.`);
  console.log(`      Current: ${syncTuning.batchSize} record(s).`);
  console.log("  Recent re-sync (days) — how many days back are always re-fetched so");
  console.log(`      edits in recent history are picked up.  Current: ${syncTuning.recentWindowDays} day(s).`);
  console.log("  Type B at any prompt to go back without changing anything.\n");

  const draft = { ...syncTuning };

  const daysInput = await askQuestion(
    `Sync window size in days [${draft.windowDays}] (${SYNC_TUNING_LIMITS.windowDays.min}-${SYNC_TUNING_LIMITS.windowDays.max}, B = back): `
  );
  if (daysInput.trim().toLowerCase() === "b") return null;
  if (daysInput.trim()) {
    const days = parseInt(daysInput.trim(), 10);
    if (isNaN(days)) {
      console.log("❌ Not a number — keeping the current value.\n");
    } else {
      draft.windowDays = clampTuningValue("windowDays", days);
      console.log(`   ✅ Sync window: ${draft.windowDays} day(s) per Tally request`);
      if (draft.windowDays > 7) {
        console.log(`   ⚠️  ${draft.windowDays} day(s) per request means Tally builds a much larger report each time.`);
        console.log("      On a large company that is what makes Tally Prime freeze or crash.");
        console.log("      1-3 days is the safe range; 7+ only for light data.");
      }
    }
  }

  const batchInput = await askQuestion(
    `Records per database batch [${draft.batchSize}] (${SYNC_TUNING_LIMITS.batchSize.min}-${SYNC_TUNING_LIMITS.batchSize.max}, B = back): `
  );
  if (batchInput.trim().toLowerCase() === "b") return null;
  if (batchInput.trim()) {
    const size = parseInt(batchInput.trim(), 10);
    if (isNaN(size)) {
      console.log("❌ Not a number — keeping the current value.\n");
    } else {
      draft.batchSize = clampTuningValue("batchSize", size);
      console.log(`   ✅ Batch size: ${draft.batchSize} record(s)`);
    }
  }

  const recentInput = await askQuestion(
    `Always re-fetch the last N days [${draft.recentWindowDays}] (${SYNC_TUNING_LIMITS.recentWindowDays.min}-${SYNC_TUNING_LIMITS.recentWindowDays.max}, 0 = off, B = back): `
  );
  if (recentInput.trim().toLowerCase() === "b") return null;
  if (recentInput.trim()) {
    const recent = parseInt(recentInput.trim(), 10);
    if (isNaN(recent)) {
      console.log("❌ Not a number — keeping the current value.\n");
    } else {
      draft.recentWindowDays = clampTuningValue("recentWindowDays", recent);
      console.log(`   ✅ Recent re-sync: last ${draft.recentWindowDays} day(s)`);
    }
  }

  const before = { ...syncTuning };
  const changed =
    draft.windowDays !== before.windowDays ||
    draft.batchSize !== before.batchSize ||
    draft.recentWindowDays !== before.recentWindowDays;

  if (!changed) {
    console.log("\nℹ️ Nothing changed — settings kept as they are.\n");
    return syncTuning;
  }

  saveSyncTuning(draft);
  console.log("\n✅ Sync performance settings saved.");
  console.log(`   Sync window   : ${syncTuning.windowDays} day(s) per Tally request`);
  console.log(`   Batch size    : ${syncTuning.batchSize} record(s) per database write`);
  console.log(`   Recent re-sync: last ${syncTuning.recentWindowDays} day(s)`);
  if (before.windowDays !== syncTuning.windowDays) {
    console.log("   ℹ️ Changing the window size re-scans the vouchers once on the new grid");
    console.log("      (existing records are updated, never duplicated).");
  }
  console.log("");
  return syncTuning;
}

// ============================================================
//  APP STATE
// ============================================================
let ACTIVE_COMPANY_GUID = null;
let ACTIVE_COMPANY_NAME = null;
let SYNC_INTERVAL = 0; // ms, set by user
let isSyncRunning = false;
// Pause & live-reconfigure support: pressing P during a sync sets SYNC_PAUSED;
// the sync finishes its current step, then the reconfiguration menu opens.
let SYNC_PAUSED = false;
let PAUSE_LISTENER_SUSPENDED = false;
const STATE_FILE = path.join(os.homedir(), "tally-agent-state.json");

// ============================================================
//  HELPERS
// ============================================================
function askQuestion(query, hideInput = false) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  if (hideInput && process.stdin.isTTY && typeof process.stdin.setRawMode === "function") {
    process.stdout.write(query);
    process.stdin.setRawMode(true);
    let input = "";

    process.stdin.on("data", (char) => {
      char = char.toString();
      if (char === "\n" || char === "\r") {
        process.stdin.setRawMode(false);
        rl.close();
        console.log();
        rl.removeAllListeners();
        return;
      }
      if (char === "\u0003") process.exit();
      input += char;
    });

    return new Promise((resolve) => {
      rl.on("close", () => resolve(input));
    });
  }

  return new Promise((resolve) =>
    rl.question(query, (answer) => {
      rl.close();
      resolve(answer);
    })
  );
}

function normalizeText(value) {
  if (!value) return null;
  if (Array.isArray(value)) return value.join(" ");
  if (typeof value === "object" && value._) return value._;
  return String(value);
}

function extractAddressValue(addr) {
  if (!addr) return null;
  if (Array.isArray(addr)) {
    return addr
      .map((a) => (typeof a === "object" ? a._ : a))
      .filter(Boolean)
      .join(" ");
  }
  if (typeof addr === "object" && addr._) return addr._;
  return String(addr);
}

function extractPhone(text) {
  const str = normalizeText(text);
  if (!str) return null;
  const matches = str.match(/\b[6-9]\d{9}\b/g);
  return matches ? matches[0] : null;
}

function extractEmail(text) {
  const str = normalizeText(text);
  if (!str) return null;
  const match = str.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0] : null;
}

function parseTallyNumber(val = "0") {
  if (val === null || val === undefined) return 0;
  if (typeof val === "object") {
    if (val._ !== undefined) val = val._;
    else return 0;
  }
  val = String(val);
  return Number(
    val
      .replace(/\(/g, "-")
      .replace(/\)/g, "")
      .replace(/,/g, "")
      .replace(/[^0-9.-]/g, "")
  );
}

// Convert "20250401" (Tally) -> "2025-04-01" (ISO)
function toISODate(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : s;
}

// Deterministic hash to keep event-style records duplicate-free
function makeHash(...parts) {
  return crypto
    .createHash("sha256")
    .update(parts.map((p) => String(p ?? "")).join("|"))
    .digest("hex");
}

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return {};
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  } catch {
    return {};
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ============================================================
//  CHECKPOINT / RESUME STATE (per-company, atomic on-disk writes)
//  Lets interrupted syncs continue from the last completed window.
// ============================================================
const SYNC_STATE_FILE = path.join(os.homedir(), "tally-agent-sync-state.json");

// ============================================================
//  SELECTIVE SYNC CONFIG  (year + month start)
//  Stored in ~/tally-agent-sync-config.json so the user only
//  needs to configure once; editable from the main menu.
// ============================================================
const SYNC_CONFIG_FILE = path.join(os.homedir(), "tally-agent-sync-config.json");

const MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"];

// 1-indexed month helpers: 1=Jan ... 12=Dec
function loadSyncConfig() {
  try {
    if (!fs.existsSync(SYNC_CONFIG_FILE)) {
      return { enabled: false, startYear: null, startMonth: 4 };
    }
    const data = JSON.parse(fs.readFileSync(SYNC_CONFIG_FILE, "utf8"));
    return {
      enabled: !!data.enabled,
      startYear: data.startYear || null,
      startMonth: data.startMonth || 4
    };
  } catch {
    return { enabled: false, startYear: null, startMonth: 4 };
  }
}

function saveSyncConfig(cfg) {
  try {
    const tmp = `${SYNC_CONFIG_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(cfg, null, 2));
    fs.renameSync(tmp, SYNC_CONFIG_FILE);
    // Keep the in-memory cache in sync so callers that only call saveSyncConfig
    // (not configureSelectiveSync) reflect the change immediately.
    if (typeof selectiveSyncConfig !== "undefined") {
      selectiveSyncConfig = loadSyncConfig();
    }
  } catch (err) {
    console.error("⚠️ Failed to save sync config:", err.message);
  }
}

// Global in-memory cache — loaded once at startup, kept in sync by saveSyncConfig()
let selectiveSyncConfig = loadSyncConfig();

// ============================================================
//  SYNC TUNING  (window days / batch size / recent re-fetch days)
//  Editable from Main Menu -> [2] More Options -> Sync Performance.
//  Persisted to ~/tally-agent-sync-tuning.json so the user configures
//  it once and it survives restarts. Environment variables only supply
//  the DEFAULTS; whatever is saved in the file always wins.
// ============================================================
const SYNC_TUNING_FILE = path.join(os.homedir(), "tally-agent-sync-tuning.json");

const SYNC_TUNING_LIMITS = {
  // Hard cap of 31 days: one request asks Tally to build a Voucher Register for
  // this whole span, and on a large company a wide span is the single most
  // reliable way to make Tally Prime run out of memory and die. Anything above
  // 7 days triggers a warning in Sync Performance.
  windowDays: { min: 1, max: 31 },
  // Capped at 2000: the widest table has 11 columns, so 2000 rows = 22,000 bind
  // parameters — comfortably under Postgres' 65,535 limit, with room to grow.
  batchSize: { min: 100, max: 2000 },
  recentWindowDays: { min: 0, max: 3650 }
};

const SYNC_TUNING_DEFAULTS = {
  // Days covered by ONE Tally voucher request. Small = small Tally responses and
  // fine-grained resume checkpoints; large = fewer requests on light data.
  windowDays: Number(process.env.VOUCHER_WINDOW_DAYS) || 3,
  // Records written to the database per batch/transaction.
  batchSize: 500,
  // How many days back are always re-fetched (edits in recent history).
  recentWindowDays: Number(process.env.RECENT_WINDOW_DAYS) || 30
};

function clampTuningValue(key, value) {
  const lim = SYNC_TUNING_LIMITS[key];
  const n = Math.round(Number(value));
  if (!lim || !Number.isFinite(n)) return SYNC_TUNING_DEFAULTS[key];
  return Math.min(lim.max, Math.max(lim.min, n));
}

function loadSyncTuning() {
  const cfg = { ...SYNC_TUNING_DEFAULTS };
  try {
    if (!fs.existsSync(SYNC_TUNING_FILE)) return cfg;
    const data = JSON.parse(fs.readFileSync(SYNC_TUNING_FILE, "utf8")) || {};
    for (const key of Object.keys(SYNC_TUNING_DEFAULTS)) {
      if (data[key] !== undefined && data[key] !== null && data[key] !== "") {
        cfg[key] = clampTuningValue(key, data[key]);
      }
    }
  } catch (err) {
    // Unreadable/corrupt file: fall back to defaults (never block startup).
  }
  return cfg;
}

function saveSyncTuning(cfg) {
  const next = {};
  for (const key of Object.keys(SYNC_TUNING_DEFAULTS)) {
    next[key] = clampTuningValue(key, cfg && cfg[key] !== undefined ? cfg[key] : SYNC_TUNING_DEFAULTS[key]);
  }
  try {
    const tmp = `${SYNC_TUNING_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(next, null, 2));
    fs.renameSync(tmp, SYNC_TUNING_FILE);
    syncTuning = next;
  } catch (err) {
    console.error("⚠️ Failed to save sync tuning:", err.message);
  }
  return syncTuning;
}

// Global in-memory cache — loaded once at startup, kept in sync by saveSyncTuning()
let syncTuning = loadSyncTuning();

// Bump whenever the request/parser semantics change so old resume checkpoints
// (which were written with a different fetch path) are treated as invalid.
const SYNC_SCHEMA = 3;

function loadSyncState() {
  try {
    if (!fs.existsSync(SYNC_STATE_FILE)) return { companies: {} };
    return JSON.parse(fs.readFileSync(SYNC_STATE_FILE, "utf8"));
  } catch {
    return { companies: {} };
  }
}

function saveSyncState(state) {
  try {
    ensureLogDir();
    const tmp = `${SYNC_STATE_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
    fs.renameSync(tmp, SYNC_STATE_FILE);
  } catch (err) {
    structuredLog("warn", "Failed to persist sync state", { error: err.message });
  }
}

function getCompanyCheckpoint(companyGuid) {
  const state = loadSyncState();
  const c = (state.companies && state.companies[companyGuid]) || {};
  for (const entity of Object.keys(c)) {
    if (c[entity] && c[entity].schema !== SYNC_SCHEMA) {
      c[entity] = {
        lastWindow: null,
        startYear: c[entity].startYear || null,
        startMonth: c[entity].startMonth || null,
        schema: SYNC_SCHEMA
      };
    }
  }
  return c;
}

// Marks a date window (label = YYYYMMDD of the window start) as fully synced.
// `startYear`/`startMonth` are recorded so a later change to SYNC_START_YEAR,
// the company start date, or the selective-sync month invalidates the old
// resume point instead of skipping unsynced data.
// `extra` lets the voucher sync record full-pass bookkeeping (contiguous
// checkpoint version, when the current full pass started, when the last full
// pass completed). Those fields are carried over on every later write for the
// same start period, so a normal per-window checkpoint never erases them.
// Callers that pass no `extra` (bills) behave exactly as before.
const CARRIED_CHECKPOINT_FIELDS = ["contiguousV", "passStartedAt", "fullPassAt", "windowDays"];
function setCompanyCheckpoint(companyGuid, entity, windowLabel, startYear, startMonth, extra = {}) {
  const state = loadSyncState();
  state.companies = state.companies || {};
  const c = (state.companies[companyGuid] = state.companies[companyGuid] || {});
  const prev = c[entity] || {};
  const samePeriod =
    prev.schema === SYNC_SCHEMA &&
    prev.startYear === (startYear || null) &&
    prev.startMonth === (startMonth || null);
  const carried = {};
  if (samePeriod) {
    for (const k of CARRIED_CHECKPOINT_FIELDS) if (prev[k] !== undefined) carried[k] = prev[k];
  }
  c[entity] = {
    ...carried,
    lastWindow: windowLabel,
    startYear: startYear || null,
    startMonth: startMonth || null,
    schema: SYNC_SCHEMA,
    updatedAt: new Date().toISOString(),
    ...extra
  };
  saveSyncState(state);
}

// Per-entity bill resume state (RECEIVABLE/PAYABLE). Bills previously had no
// checkpoint, so the first run after this change always does a full (windowed)
// sync, then resumes from the last completed window like every other entity.
function getBillSyncState(billType) {
  const key = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";
  const checkpoint = getCompanyCheckpoint(ACTIVE_COMPANY_GUID)[key] || {};
  return {
    lastWindow: checkpoint.lastWindow || null,
    startYear: checkpoint.startYear || null,
    startMonth: checkpoint.startMonth || null,
    schema: checkpoint.schema || null
  };
}

function saveBillSyncState(billType, windowLabel, startYear, startMonth) {
  const key = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";
  setCompanyCheckpoint(ACTIVE_COMPANY_GUID, key, windowLabel, startYear, startMonth);
}

// ============================================================
//  SYNC STATE (DB-backed incremental sync metadata)
// ============================================================
async function getSyncState(companyGuid, entity) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM sync_state WHERE company_guid = $1 AND entity = $2",
      [companyGuid, entity]
    );
    return rows[0] || null;
  } catch {
    return null;
  }
}

async function upsertSyncState(companyGuid, entity, { lastSyncAt, lastWindow, recordCount, checksum, meta } = {}) {
  const existing = await getSyncState(companyGuid, entity);
  const now = new Date().toISOString();
  if (existing) {
    const fields = [];
    const vals = [];
    let idx = 3;
    if (lastSyncAt !== undefined) { fields.push(`last_sync_at = $${idx++}`); vals.push(lastSyncAt); }
    if (lastWindow !== undefined) { fields.push(`last_window = $${idx++}`); vals.push(lastWindow); }
    if (recordCount !== undefined) { fields.push(`record_count = $${idx++}`); vals.push(recordCount); }
    if (checksum !== undefined) { fields.push(`checksum = $${idx++}`); vals.push(checksum); }
    if (meta !== undefined) { fields.push(`meta = $${idx++}`); vals.push(JSON.stringify(meta)); }
    if (fields.length === 0) return;
    vals.unshift(companyGuid, entity);
    await pool.query(`UPDATE sync_state SET ${fields.join(", ")} WHERE company_guid = $1 AND entity = $2`, vals);
  } else {
    await pool.query(
      `INSERT INTO sync_state (company_guid, entity, last_sync_at, last_window, record_count, checksum, meta)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [companyGuid, entity, lastSyncAt || now, lastWindow || null, recordCount || 0, checksum || null, JSON.stringify(meta || {})]
    );
  }
}

function computeGuidChecksum(guids) {
  if (!guids.length) return null;
  const sorted = [...guids].sort();
  let hash = 0;
  for (const g of sorted) {
    for (let i = 0; i < g.length; i++) {
      hash = ((hash << 5) - hash + g.charCodeAt(i)) | 0;
    }
  }
  return hash.toString(36);
}

// ============================================================
//  STRUCTURED LOGGING & SAFE DB WRITES
// ============================================================
const LOG_DIR = path.join(os.homedir(), "tally-agent-logs");
let SYNC_RUN_ID = null;

function ensureLogDir() {
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  } catch {}
}

function todayStamp() {
  return new Date().toISOString().slice(0, 10);
}

function logFilePath(prefix) {
  return path.join(LOG_DIR, `${prefix}-${todayStamp()}.log`);
}

function writeJsonLine(file, entry) {
  ensureLogDir();
  try {
    fs.appendFileSync(file, JSON.stringify(entry) + "\n");
  } catch {}
}

function structuredLog(level, msg, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    msg,
    syncId: SYNC_RUN_ID,
    ...meta
  };
  writeJsonLine(logFilePath("sync"), entry);
  // Detailed technical logs are shown on console ONLY in Developer Mode.
  // They are ALWAYS written to the log file regardless of mode.
  if (!isDeveloperMode) return;
  const suffix = Object.keys(meta).length ? " " + JSON.stringify(meta) : "";
  console.log(`[${level.toUpperCase()}] ${msg}${suffix}`);
}

function logFailure(entity, identifier, error, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    syncId: SYNC_RUN_ID,
    entity,
    identifier,
    error: (error && (error.message || error)) || String(error),
    stack: (error && error.stack) || null,
    ...meta
  };
  writeJsonLine(logFilePath("failures"), entry);
  structuredLog("error", `[${entity}] Failed record: ${identifier}`, {
    error: entry.error
  });
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)}s`;
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
}

function fmtBytes(n) {
  if (!n) return "0 B";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function summarizeRecord(record) {
  if (!record || typeof record !== "object") return record;
  const { payload, ...rest } = record;
  return rest;
}

const SYNC_SUMMARY = {
  startedAt: null,
  endedAt: null,
  companiesSynced: 0,
  companyGuids: [],
  entities: {},
  voucherTypeBreakdown: null,
  companies: [] // per-company summary buckets: { guid, name, entities, voucherTypeBreakdown }
};

// Points at the bucket of the company currently being synced so entity
// stats are recorded both in the aggregate and per-company views.
let CURRENT_COMPANY_STATS = null;

function recordEntityStats(entity, { fetched = 0, saved = 0, failed = 0, skipped = 0, newCount = null } = {}) {
  const applyTo = (bucket) => {
    const prev = bucket[entity] || { fetched: 0, saved: 0, failed: 0, skipped: 0, newCount: 0 };
    bucket[entity] = {
      fetched: prev.fetched + fetched,
      saved: prev.saved + saved,
      failed: prev.failed + failed,
      skipped: prev.skipped + skipped,
      // Real newly-inserted count from the upsert result; falls back to
      // saved only when a caller does not provide it.
      newCount: prev.newCount + (newCount === null ? saved : newCount)
    };
  };
  applyTo(SYNC_SUMMARY.entities);
  if (CURRENT_COMPANY_STATS) applyTo(CURRENT_COMPANY_STATS.entities);
}

// Friendly display labels for entities in the final summary.
function entityLabel(entityKey) {
  const found = DATA_TYPES.find((d) => d.key === entityKey);
  return found ? found.label : entityKey;
}

// Builds the explanatory note for unconfirmed draft vouchers excluded from
// the breakdown total (optional / post-dated). Cancelled drafts are counted
// in the breakdown (Tally Prime counts them). Only non-cancelled drafts are
// hidden — they are still stored in the database.
function draftNote(count, drafts = null) {
  if (!count || count <= 0) return null;
  let note = `Excluded ${count.toLocaleString()} draft voucher(s) (optional, not confirmed, not cancelled) from the Total, matching Tally Prime's voucher count. They are still stored in the database. Cancelled drafts are included in the Cancelled count above, also matching Tally Prime.`;
  if (drafts && drafts.length > 0) {
    const fmtDate = (d) => {
      if (!d) return "no date";
      if (typeof d === "string") return d.slice(0, 10);
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${dd}`;
    };
    const MAX_LIST = 50;
    const shown = drafts.slice(0, MAX_LIST);
    const out = [`Draft vouchers (${drafts.length}):`];
    shown.forEach((d, i) => {
      const num = d.voucher_number || d.reference_no || "(no number)";
      const type = d.voucher_type || "-";
      const date = fmtDate(d.voucher_date);
      const flags = [];
      if (d.is_optional) flags.push("Optional");
      else if (d.is_postdated) flags.push("Post-dated");
      if (d.cancelled) flags.push("Cancelled");
      const status = flags.length ? flags.join(", ") : "Draft";
      const amt =
        d.net_amount !== undefined && d.net_amount !== null && d.net_amount !== ""
          ? `Rs. ${Number(d.net_amount).toLocaleString("en-IN")}`
          : "-";
      const narRaw = d.narration ? String(d.narration) : "";
      const nar = narRaw.length > 60 ? narRaw.slice(0, 57) + "..." : narRaw || "-";
      out.push(`  • Voucher ${i + 1}:`);
      out.push(`      Vch No.   : ${num}`);
      out.push(`      Type      : ${type}`);
      out.push(`      Date      : ${date}`);
      out.push(`      Status    : ${status}`);
      out.push(`      Amount    : ${amt}`);
      out.push(`      Narration : ${nar}`);
    });
    if (drafts.length > MAX_LIST) {
      out.push(`  …and ${(drafts.length - MAX_LIST).toLocaleString()} more`);
    }
    note += `\n     ${out.join("\n     ")}`;
  }
  return note;
}

// rows: [{ type, active, cancelled, total? }] — numbers mirror Tally Prime's
// "Types of Vouchers" report exactly:
//   Active    = confirmed (non-optional) vouchers that are not cancelled
//   Cancelled = ALL cancelled vouchers (optional/draft cancelled ones too) —
//               this is what Tally Prime shows in its "Cancelled" column
//   Total     = confirmed vouchers (active + confirmed-cancelled) — this is
//               Tally Prime's "Total" per voucher type; optional drafts are
//               NOT counted by Tally and so are excluded here as well.
// hiddenNote: optional extra line listing un-confirmed draft vouchers that
// are stored in the DB but not counted (matching Tally Prime).
function printVoucherTypeBreakdown(rows, hiddenNote) {
  if (!rows || !rows.length) return;
  console.log("");
  console.log("Voucher Type Breakdown (Active / Cancelled / Total):");
  console.log("  Matches Tally Prime's Types-of-Vouchers count: Total = confirmed vouchers,");
  console.log("  Cancelled = all cancelled (including cancelled drafts).");
  let gA = 0, gC = 0, gT = 0;
  for (const r of rows) {
    const total = r.total ?? r.active + r.cancelled;
    console.log(`  • ${r.type}: ${r.active.toLocaleString()} / ${r.cancelled.toLocaleString()} / ${total.toLocaleString()}`);
    gA += r.active;
    gC += r.cancelled;
    gT += total;
  }
  console.log(`  ── TOTAL: ${gA.toLocaleString()} / ${gC.toLocaleString()} / ${gT.toLocaleString()}`);
  if (hiddenNote) console.log(`  ℹ️ ${hiddenNote}`);
}

async function printSyncSummary(syncStart) {
  const dur = Date.now() - syncStart;
  structuredLog("info", "Sync summary", {
    startedAt: SYNC_SUMMARY.startedAt ? SYNC_SUMMARY.startedAt.toISOString() : null,
    endedAt: SYNC_SUMMARY.endedAt ? SYNC_SUMMARY.endedAt.toISOString() : null,
    durationMs: dur,
    duration: formatDuration(dur),
    companies: SYNC_SUMMARY.companiesSynced,
    entities: SYNC_SUMMARY.entities
  });

  // One completely separate summary block per company (never mixed).
  const companyBlocks = SYNC_SUMMARY.companies.length
    ? SYNC_SUMMARY.companies
    : [{ guid: null, name: null, entities: SYNC_SUMMARY.entities, voucherTypeBreakdown: SYNC_SUMMARY.voucherTypeBreakdown }];
  const multipleCompanies = companyBlocks.length > 1;

  for (let ci = 0; ci < companyBlocks.length; ci++) {
    const cs = companyBlocks[ci];
    const guids = cs.guid ? [cs.guid] : SYNC_SUMMARY.companyGuids;

    // In-DB counts for THIS company only (source of truth stays the DB).
    let dbTotals = {};
    try {
      if (pool && guids.length > 0) {
        const res = await pool.query(
          `SELECT
             (SELECT count(*)::int FROM vouchers WHERE company_guid = ANY($1)
                AND COALESCE(payload->'ISOPTIONAL'->>0, 'No') <> 'Yes') AS vouchers,
             (SELECT count(*)::int FROM invoices WHERE company_guid = ANY($1)) AS invoices,
             (SELECT count(*)::int FROM ledgers WHERE company_guid = ANY($1)) AS ledgers,
             (SELECT count(*)::int FROM orders WHERE company_guid = ANY($1)) AS orders,
             (SELECT count(*)::int FROM inventory_items WHERE company_guid = ANY($1)) AS inventory_items,
             (SELECT count(*)::int FROM bills WHERE company_guid = ANY($1) AND bill_type = 'RECEIVABLE') AS bills_receivable,
             (SELECT count(*)::int FROM bills WHERE company_guid = ANY($1) AND bill_type = 'PAYABLE') AS bills_payable`,
          [guids]
        );
        dbTotals = res.rows[0] || {};
      }
    } catch (e) {
      dbTotals = {};
    }

    console.log("");
    console.log(multipleCompanies ? `========== COMPANY ${ci + 1} SUMMARY ==========` : "========== SYNC SUMMARY ==========");
    if (cs.name) console.log(`Company : ${cs.name}`);
    if (isDeveloperMode) {
      console.log(`Started   : ${SYNC_SUMMARY.startedAt ? SYNC_SUMMARY.startedAt.toISOString() : "N/A"}`);
      console.log(`Ended     : ${SYNC_SUMMARY.endedAt ? SYNC_SUMMARY.endedAt.toISOString() : "N/A"}`);
      console.log(`Duration  : ${formatDuration(dur)}`);
      console.log(`Companies : ${SYNC_SUMMARY.companiesSynced}`);
    }
    console.log("");
    console.log(
      "Entity".padEnd(26) + " " + "Fetched".padStart(7) + " " + "New".padStart(6) +
      " " + "Failed".padStart(8) + " " + "Skipped".padStart(8) + " " + "In DB".padStart(8)
    );
    console.log("-".repeat(72));

    // Entities relevant to the selected sync only ("companies" detection is
    // internal bookkeeping and not shown).
    const displayEntities = Object.entries(cs.entities).filter(([entity]) => entity !== "companies");
    let totalNewRecords = 0;
    for (const [entity, s] of displayEntities) {
      const total = dbTotals[entity] !== undefined ? dbTotals[entity] : "-";
      totalNewRecords += s.newCount || 0;
      console.log(
        `${entityLabel(entity).padEnd(26)} ${String(s.fetched).padStart(7)} ${String(s.newCount || 0).padStart(6)} ${String(s.failed).padStart(8)} ${String(s.skipped).padStart(8)} ${String(total).padStart(8)}`
      );
    }

    // 🆕 New Records section — real newly-inserted counts of THIS cycle.
    if (displayEntities.length > 0) {
      console.log("");
      console.log("🆕 NEW RECORDS FOUND");
      for (const [entity, s] of displayEntities) {
        console.log(`   • ${entityLabel(entity)}: ${s.newCount || 0} new`);
      }
      console.log("");
      console.log(`📊 Total New Records: ${totalNewRecords}`);
      if (totalNewRecords === 0) {
        console.log("");
        console.log("ℹ️ No new records found. Existing data is up to date.");
      }
    }

    // Voucher Type Breakdown — ALWAYS query DB for active+cancelled counts.
    // With is_cancelled column, both counts come directly from the DB (source of truth).
    // Queried per company so multi-company summaries never mix data.
    try {
      if (pool && guids.length > 0) {
        const { rows: dbRows } = await pool.query(
          `SELECT COALESCE(voucher_type, 'Unknown') AS voucher_type,
                  count(*) FILTER (WHERE COALESCE(payload->'ISOPTIONAL'->>0, 'No') <> 'Yes'
                                   AND COALESCE(payload->'ISCANCELLED'->>0, 'No') <> 'Yes')::int AS active,
                  count(*) FILTER (WHERE COALESCE(payload->'ISCANCELLED'->>0, 'No') = 'Yes')::int AS cancelled,
                  count(*) FILTER (WHERE COALESCE(payload->'ISOPTIONAL'->>0, 'No') <> 'Yes')::int AS confirmed
           FROM vouchers
          WHERE company_guid = ANY($1)
           GROUP BY voucher_type ORDER BY confirmed DESC, cancelled DESC`,
          [guids]
        );
        // Unconfirmed draft vouchers excluded above — surface the count so the
        // numbers reconcile with Tally Prime's on-screen total.
        // Only non-cancelled drafts are excluded from the Total; cancelled
        // drafts appear in the Cancelled count above (Tally Prime counts every
        // cancelled voucher, even drafts).
        let hiddenNote = null;
        try {
          const { rows: hidRows } = await pool.query(
            `SELECT reference_no,
                    voucher_type,
                    voucher_date,
                    net_amount,
                    COALESCE(payload->'VOUCHERNUMBER'->>0, '') AS voucher_number,
                    COALESCE(payload->'NARRATION'->>0, '') AS narration,
                    COALESCE(payload->'ISOPTIONAL'->>0, 'No') = 'Yes' AS is_optional,
                    COALESCE(payload->'ISPOSTDATED'->>0, 'No') = 'Yes' AS is_postdated,
                    COALESCE(payload->'ISCANCELLED'->>0, 'No') = 'Yes' AS is_cancelled
             FROM vouchers
            WHERE company_guid = ANY($1)
              AND COALESCE(payload->'ISOPTIONAL'->>0, 'No') = 'Yes'
              AND COALESCE(payload->'ISCANCELLED'->>0, 'No') <> 'Yes'
            ORDER BY voucher_date NULLS LAST`,
            [guids]
          );
          hiddenNote = draftNote(
            hidRows.length,
            hidRows.map((r) => ({ ...r, cancelled: r.is_cancelled }))
          );
        } catch {}
        const mapped = dbRows.map((r) => ({
          type: r.voucher_type,
          active: r.active,
          cancelled: r.cancelled,
          total: r.confirmed
        }));
        printVoucherTypeBreakdown(mapped, hiddenNote);
      } else if (cs.voucherTypeBreakdown && cs.voucherTypeBreakdown.length > 0) {
        // Fallback: last-known breakdown captured during this cycle.
        const d = cs.voucherDrafts;
        // Only non-cancelled drafts are excluded from the breakdown;
        // cancelled drafts are counted in the type breakdown above.
        const draftCount = d ? d.active : 0;
        const draftList = d?.list ? d.list.filter((r) => !r.cancelled) : null;
        printVoucherTypeBreakdown(cs.voucherTypeBreakdown, draftNote(draftCount, draftList));
      }
    } catch (e) {}

    const qCount = quarantineManager.count(cs.name || ACTIVE_COMPANY_NAME);
    if (qCount > 0) {
      console.log(`\n⚠️  ${qCount} voucher(s) quarantined (Tally crashes when exporting them). See ~/tally-agent-quarantine.json`);
      console.log("   Run Repair (Alt+F3 -> Data -> Repair) on the company in Tally Prime if these continue to fail.");
    }

    console.log("");
    console.log("========================================");
  }

  // Incremental sync state — technical checkpoint details, Developer Mode only.
  if (isDeveloperMode && pool && SYNC_SUMMARY.companyGuids.length > 0) {
    try {
      const { rows: stateRows } = await pool.query(
        `SELECT entity, last_sync_at, record_count, meta FROM sync_state
         WHERE company_guid = ANY($1) ORDER BY entity`,
        [SYNC_SUMMARY.companyGuids]
      );
      if (stateRows.length > 0) {
        console.log("");
        console.log("Incremental Sync State:");
        for (const r of stateRows) {
          const lastSync = r.last_sync_at ? new Date(r.last_sync_at).toLocaleString() : "never";
          const meta = r.meta || {};
          const changed = meta.changed === true ? " ⚡ changed" : meta.changed === false ? " (unchanged)" : "";
          const windows = meta.windowsProcessed !== undefined
            ? ` [${meta.windowsProcessed} windows${meta.windowsResumed ? ", " + meta.windowsResumed + " resumed" : ""}${meta.windowsReFetchedRecent ? ", " + meta.windowsReFetchedRecent + " recent" : ""}]`
            : "";
          console.log(`  • ${r.entity}: last sync ${lastSync}, ${r.record_count} records${changed}${windows}`);
        }
      }
    } catch {}
  }
}

// ============================================================
//  TALLY FETCH WRAPPER (retries + structured logs)
// ============================================================
async function fetchTallyXml(xmlData, { entity = "tally", timeout = TALLY_TIMEOUT_MS } = {}) {
  const t0 = Date.now();
  structuredLog("info", `[${entity}] FETCH: requesting data from Tally`);

  const heartbeatMs = 5000;
  const heartbeat = setInterval(() => {
    const elapsed = formatDuration(Date.now() - t0);
    const timeoutTxt = timeout ? ` (timeout ${Math.round(timeout / 1000)}s)` : "";
    devLog(`[INFO] [${entity}] FETCH: still waiting for Tally... ${elapsed} elapsed${timeoutTxt}`);
    const entry = {
      ts: new Date().toISOString(),
      level: "info",
      msg: `[${entity}] FETCH: still waiting for Tally... ${elapsed} elapsed`,
      syncId: SYNC_RUN_ID
    };
    writeJsonLine(logFilePath("sync"), entry);
  }, heartbeatMs);
  if (heartbeat.unref) heartbeat.unref();

  try {
    const res = await postTallyXml(xmlData, { timeout });
    structuredLog("info", `[${entity}] FETCH: response received`, {
      duration: formatDuration(Date.now() - t0),
      size: fmtBytes(res && res.data ? res.data.length : 0)
    });
    return res;
  } catch (err) {
    structuredLog("error", `[${entity}] FETCH: failed`, {
      error: err.message,
      duration: formatDuration(Date.now() - t0)
    });
    throw err;
  } finally {
    clearInterval(heartbeat);
  }
}

// Tally emits invalid XML control characters / character references (e.g.
// &#4;) as field separators. A strict SAX parser errors on them, which can
// garble or drop voucher data around the bad character. Same helper as the
// proven working agent: strip exactly that class before parsing.
const TALLY_ILLEGAL_CHAR_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F]/g;
const TALLY_ILLEGAL_CHARREF_RE =
  /&#(?:0*[0-8]|0*1[12]|0*1[4-9]|0*2[0-9]|0*3[01]);|&#x0*(?:[0-8bBcC]|[eEfF]|1[0-9a-fA-F]);/g;

function sanitizeTallyXml(xmlText) {
  if (!xmlText || typeof xmlText !== "string") return xmlText;
  return xmlText
    .replace(TALLY_ILLEGAL_CHARREF_RE, "")
    .replace(TALLY_ILLEGAL_CHAR_RE, "");
}

// Number of companies currently open in Tally (set by detectCompanies()).
// With more than one open, Tally can ignore SVFROMDATE/SVTODATE on the Voucher
// Register (and may serve whichever company is active in its UI), so every
// "3-day" window becomes a full-book export. Measured by the working agent.
let TALLY_OPEN_COMPANY_COUNT = 0;

// ============================================================
//  STREAMING VOUCHER FETCH (memory-safe for very large data)
//  Reads Tally's response chunk-by-chunk through a SAX parser,
//  emitting one VOUCHER at a time. No DOM tree is ever built, so
//  memory stays flat no matter the response size (300MB, 1GB, ...).
// ============================================================
function saxVoucherStream(onVoucher) {
  const stack = [];
  const parser = sax.parser(true, { trim: true });
  const drained = [];
  parser.seenLineError = false;

  function buildValue(el) {
    if (!el.children.length) return (el.text || "").trim();
    const obj = {};
    const groups = {};
    for (const child of el.children) {
      (groups[child.name] = groups[child.name] || []).push(child);
    }
    for (const name of Object.keys(groups)) {
      obj[name] = groups[name].map((c) => buildValue(c));
    }
    return obj;
  }

  parser.onopentag = (node) => {
    if (node.name === "LINEERROR") parser.seenLineError = true;
    stack.push({ name: node.name, children: [], text: "" });
  };

  parser.ontext = (t) => {
    if (stack.length && t) {
      const top = stack[stack.length - 1];
      if (!top.children.length) top.text += t;
    }
  };

  parser.onclosetag = () => {
    const el = stack.pop();
    const parent = stack[stack.length - 1];
    if (!parent) return;
    if (el.name === "VOUCHER") {
      drained.push(buildValue(el));
    } else {
      parent.children.push(el);
    }
  };

  parser.onerror = () => {
    try {
      parser.resume();
    } catch {}
  };

  // Returns the vouchers completed since the last call (clears the buffer).
  parser.consumeDrained = () => {
    if (!drained.length) return [];
    const items = drained.slice();
    drained.length = 0;
    return items;
  };

  return parser;
}

// SAX parser for the "Bills Receivable"/"Bills Payable" report XML. Tally emits
// bill data as PARALLEL arrays (BILLFIXED[i], BILLCL[i], BILLDUE[i],
// BILLOVERDUE[i] for the i-th bill). We collect each tag into its own array —
// this works no matter whether Tally interleaves the tags per bill or groups all
// BILLFIXED first — and zip them by index afterwards. Tag names are matched at
// ANY nesting depth (top-level under ENVELOPE or nested under BODY/DATA), so the
// parser is robust to Tally layout differences. Memory stays flat: only the
// bill elements are kept, never a full DOM tree of the response.
function saxBillsStream() {
  const stack = [];
  const parser = sax.parser(true, { trim: true });
  const result = { fixed: [], cl: [], due: [], over: [] };
  parser.seenLineError = false;

  function buildValue(el) {
    if (!el.children.length) return (el.text || "").trim();
    const obj = {};
    const groups = {};
    for (const child of el.children) {
      (groups[child.name] = groups[child.name] || []).push(child);
    }
    for (const name of Object.keys(groups)) {
      obj[name] = groups[name].map((c) => buildValue(c));
    }
    return obj;
  }

  parser.onopentag = (node) => {
    if (node.name === "LINEERROR") parser.seenLineError = true;
    stack.push({ name: node.name, children: [], text: "" });
  };

  parser.ontext = (t) => {
    if (stack.length && t) {
      const top = stack[stack.length - 1];
      if (!top.children.length) top.text += t;
    }
  };

  parser.onclosetag = () => {
    const el = stack.pop();
    const parent = stack[stack.length - 1];
    if (el.name === "BILLFIXED") {
      result.fixed.push(buildValue(el));
      return;
    }
    if (el.name === "BILLCL") {
      result.cl.push(buildValue(el));
      return;
    }
    if (el.name === "BILLDUE") {
      result.due.push(buildValue(el));
      return;
    }
    if (el.name === "BILLOVERDUE") {
      result.over.push(buildValue(el));
      return;
    }
    if (parent) parent.children.push(el);
  };

  parser.onerror = () => {
    try {
      parser.resume();
    } catch {}
  };

  return { parser, result };
}

// Streams the Bills Receivable/Payable report from Tally through a SAX parser
// and returns the zipped bill records (fixed + cl + due + over). Bounded to a
// single date window by the caller, so Tally never has to build a full-history
// report (the root cause of the bills sync hang). An empty/`<LINEERROR>`
// response is NOT an error — it just means "no bill data in this window" — so
// it is returned as zero records instead of being retried/raised.
async function fetchBillsStream(xmlData, { entity = "bills", timeout = TALLY_TIMEOUT_MS } = {}) {
  return withTallyThrottle(() => fetchBillsStreamRaw(xmlData, { entity, timeout }), entity);
}

async function fetchBillsStreamRaw(xmlData, { entity = "bills", timeout = TALLY_TIMEOUT_MS } = {}) {
  const t0 = Date.now();
  const retries = 3;
  let lastErr = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const heartbeat = setInterval(() => {
      const elapsed = formatDuration(Date.now() - t0);
      devLog(`[INFO] [${entity}] FETCH: still streaming from Tally... ${elapsed} elapsed (attempt ${attempt}/${retries})`);
      const entry = {
        ts: new Date().toISOString(),
        level: "info",
        msg: `[${entity}] FETCH: still streaming from Tally... ${elapsed} elapsed`,
        syncId: SYNC_RUN_ID
      };
      writeJsonLine(logFilePath("sync"), entry);
    }, 5000);
    if (heartbeat.unref) heartbeat.unref();

    try {
      const res = await tallyHttp.post(TALLY_URL, xmlData, {
        headers: { "Content-Type": "text/xml" },
        timeout,
        responseType: "stream"
      });

      const { parser, result } = saxBillsStream();
      let bytes = 0;

      await new Promise((resolve, reject) => {
        res.data.on("data", (chunk) => {
          res.data.pause();
          bytes += chunk.length;
          try {
            parser.write(chunk);
          } catch (e) {
            if (!res.data.destroyed) res.data.destroy();
            reject(e);
            return;
          }
          if (!res.data.destroyed) res.data.resume();
        });
        res.data.on("end", () => {
          try {
            parser.close();
            resolve();
          } catch (e) {
            reject(e);
          }
        });
        res.data.on("error", (err) => reject(err));
      });

      clearInterval(heartbeat);

      if (parser.seenLineError) {
        structuredLog("warn", `[${entity}] FETCH: Tally returned LINEERROR for window (no bill data)`, {
          duration: formatDuration(Date.now() - t0),
          size: fmtBytes(bytes)
        });
        return { records: [], bytes, lineError: true };
      }

      const records = [];
      for (let i = 0; i < result.fixed.length; i++) {
        records.push({
          fixed: result.fixed[i],
          cl: result.cl[i],
          due: result.due[i],
          over: result.over[i]
        });
      }

      structuredLog("info", `[${entity}] FETCH: response received`, {
        duration: formatDuration(Date.now() - t0),
        size: fmtBytes(bytes),
        records: records.length,
        attempt
      });
      return { records, bytes, lineError: false };
    } catch (err) {
      clearInterval(heartbeat);
      lastErr = err;
      structuredLog("warn", `[${entity}] FETCH: stream failed (attempt ${attempt}/${retries})`, {
        error: err.message
      });
      if (attempt < retries) await sleep(2000);
    }
  }
  throw lastErr;
}

// Streams Tally's XML response through a SAX parser, emitting one VOUCHER at a
// time. When `onBatch` is provided, vouchers are buffered into small batches and
// `onBatch(items)` is awaited (with backpressure) so the caller can persist each
// batch to the DB without ever holding a whole window in memory. Memory stays
// flat no matter the response size.
// A request that timed out is NOT retried: aborting the client does not cancel
// Tally's server-side work (Tally's XML API is single-threaded), so re-sending
// the same heavy request only queues more work behind it (working-agent finding).
function isTallyTimeoutError(err) {
  if (!err) return false;
  return !!err.isTallyTimeout || err.code === "ECONNABORTED" || /timeout of \d+ms exceeded/i.test(err.message || "");
}

async function fetchVouchersStream(xmlData, onVoucher, opts = {}) {
  return withTallyThrottle(() => fetchVouchersStreamRaw(xmlData, onVoucher, opts), "vouchers");
}

async function fetchVouchersStreamRaw(xmlData, onVoucher, { batchSize = 500, onBatch = null, timeout = TALLY_TIMEOUT_MS, retries = 3 } = {}) {
  const t0 = Date.now();
  let lastErr = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const attemptStart = Date.now();
    let bytes = 0;
    let firstByteAt = null;

    // Heartbeat: says whether we are still WAITING for Tally's first byte
    // (Tally is building the report) or RECEIVING data, with bytes so far.
    // Local only — it never sends anything to Tally.
    const heartbeat = setInterval(() => {
      const elapsed = formatDuration(Date.now() - t0);
      const status = firstByteAt
        ? `receiving data from Tally... ${fmtBytes(bytes)} so far, ${elapsed} elapsed`
        : `waiting for Tally to start responding... ${elapsed} elapsed`;
      devLog(`[INFO] [vouchers] FETCH: ${status} (attempt ${attempt}/${retries})`);
      writeJsonLine(logFilePath("sync"), {
        ts: new Date().toISOString(),
        level: "info",
        msg: `[vouchers] FETCH: ${status} (attempt ${attempt}/${retries})`,
        syncId: SYNC_RUN_ID
      });
      progressStatus(firstByteAt ? `receiving ${fmtBytes(bytes)}` : `waiting for Tally ${elapsed}`);
    }, 5000);
    if (heartbeat.unref) heartbeat.unref();

    try {
      const res = await tallyHttp.post(TALLY_URL, xmlData, {
        headers: { "Content-Type": "text/xml" },
        timeout,
        responseType: "stream"
      });

      const parser = saxVoucherStream(onVoucher);
      // Decode bytes as UTF-8 without splitting multi-byte characters across
      // chunk boundaries, then strip Tally's invalid characters before parsing.
      const decoder = new StringDecoder("utf8");
      let entityCarry = "";
      let pending = [];
      let pendingCount = 0;
      let flushing = false;
      let ended = false;

      // Drain the buffered vouchers into the caller-provided batch handler.
      // `await onBatch(...)` runs while the HTTP stream is paused (backpressure),
      // so memory never grows beyond ~batchSize vouchers.
      async function drain() {
        while (pending.length) {
          const items = pending;
          pending = [];
          pendingCount = 0;
          if (onVoucher) {
            for (const v of items) onVoucher(v);
          }
          if (onBatch) {
            await onBatch(items);
          }
        }
      }

      // Same technique as the working agent: an "&..." entity can straddle a
      // chunk boundary, so hold back a short unterminated tail for the next chunk.
      function feed(text, final) {
        let t = entityCarry + text;
        entityCarry = "";
        if (!final) {
          const lastAmp = t.lastIndexOf("&");
          if (lastAmp !== -1 && t.indexOf(";", lastAmp) === -1 && t.length - lastAmp <= 12) {
            entityCarry = t.slice(lastAmp);
            t = t.slice(0, lastAmp);
          }
        }
        if (t) parser.write(sanitizeTallyXml(t));
      }

      await new Promise((resolve, reject) => {
        let settled = false;
        const done = (fn, v) => { if (!settled) { settled = true; fn(v); } };
        res.data.on("data", (chunk) => {
          res.data.pause();
          if (!firstByteAt) firstByteAt = Date.now();
          bytes += chunk.length;
          try {
            feed(decoder.write(chunk), false);
            for (const v of parser.consumeDrained()) {
              pending.push(v);
              pendingCount++;
            }
            if (pendingCount >= batchSize && !flushing) {
              flushing = true;
              drain()
                .catch((e) => {
                  if (!res.data.destroyed) res.data.destroy();
                  done(reject, e);
                })
                .finally(() => {
                  flushing = false;
                  if (!res.data.destroyed) res.data.resume();
                });
            } else if (!res.data.destroyed) {
              res.data.resume();
            }
          } catch (e) {
            if (!res.data.destroyed) res.data.destroy();
            done(reject, e);
          }
        });
        res.data.on("end", async () => {
          ended = true;
          try {
            feed(decoder.end(), true);
            parser.close();
            for (const v of parser.consumeDrained()) pending.push(v);
            await drain();
            done(resolve);
          } catch (e) {
            done(reject, e);
          }
        });
        res.data.on("error", (err) => done(reject, err));
        // If the connection is cut mid-response (e.g. the idle timeout aborted
        // it, or Tally closed), Node may emit only "close". Never hang on it.
        res.data.on("close", () => {
          if (!ended) {
            const e = new Error("Tally connection closed before the response completed");
            if (timeout && Date.now() - attemptStart >= timeout) e.isTallyTimeout = true;
            done(reject, e);
          }
        });
      });

      clearInterval(heartbeat);
      progressStatus(null);
      if (parser.seenLineError) {
        throw new Error("Tally returned LINEERROR (report request failed)");
      }
      structuredLog("info", "[vouchers] FETCH: response received", {
        duration: formatDuration(Date.now() - t0),
        waitedForFirstByte: firstByteAt ? formatDuration(firstByteAt - attemptStart) : "n/a",
        size: fmtBytes(bytes),
        attempt
      });
      return { bytes };
    } catch (err) {
      clearInterval(heartbeat);
      progressStatus(null);
      lastErr = err;
      const timedOut = isTallyTimeoutError(err);
      if (timedOut) err.isTallyTimeout = true;
      // LINEERROR is Tally's own "I could not build this report" answer. It is
      // deterministic, so retrying only hammers Tally with the same doomed
      // request — give up immediately and let the window fail gracefully.
      const lineError = /LINEERROR/i.test(err.message || "");
      structuredLog("warn", `[vouchers] FETCH: stream failed (attempt ${attempt}/${retries})${timedOut ? " — timed out, NOT retrying (Tally is still busy with this request)" : ""}${lineError ? " — report rejected by Tally, NOT retrying" : ""}`, {
        error: err.message,
        receivedBeforeFailure: fmtBytes(bytes)
      });
      if (timedOut || lineError) break;
      if (attempt < retries) await sleep(2000);
    }
  }
  throw lastErr;
}

// ============================================================
//  SAFE BATCH WRITES (per-record isolation + retries)
// ============================================================
function buildBatchValues(rows, columns) {
  const placeholders = rows
    .map((_, rIdx) => `(${columns.map((_, cIdx) => `$${rIdx * columns.length + cIdx + 1}`).join(",")})`)
    .join(",");
  const values = rows.flatMap((r) =>
    columns.map((c) => (r[c] === undefined || r[c] === "" ? null : r[c]))
  );
  return { placeholders, values };
}

function buildUpsertStatement({ table, columns, rows, conflict, updateColumns }) {
  const { placeholders, values } = buildBatchValues(rows, columns);
  const conflictTarget = Array.isArray(conflict) ? `(${conflict.join(", ")})` : conflict;
  let sql;
  if (updateColumns && updateColumns.length) {
    const setClause = updateColumns.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ");
    // updated_at reflects the last time the row was refreshed from Tally.
    // Only touched when the table actually manages that column — older
    // databases may not have it on every table (e.g. bills created before
    // the column existed), and referencing it there would fail every upsert.
    // RETURNING (xmax = 0) is a read-only marker that lets us distinguish rows
    // actually INSERTED by this statement (xmax = 0) from rows that already
    // existed and were only UPDATED (xmax <> 0). No database behavior change.
    const touchTimestamp = columns.includes("updated_at") || updateColumns.includes("updated_at");
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO UPDATE SET ${setClause}${touchTimestamp ? ', "updated_at" = NOW()' : ""}
           RETURNING (xmax = 0) AS "_is_insert"`;
  } else {
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO NOTHING`;
  }
  return { sql, values };
}

function buildInsertStatement({ table, columns, rows }) {
  const { placeholders, values } = buildBatchValues(rows, columns);
  const sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
               VALUES ${placeholders}`;
  return { sql, values };
}

let _savepointCounter = 0;

function isTransientDbError(err) {
  if (!err) return false;
  const code = String(err.code || "");
  if (/^(08|40|57)/.test(code)) return true;
  const msg = `${err.message || ""} ${err.code || ""}`;
  return /(ECONNRESET|ETIMEDOUT|EPIPE|EHOSTUNREACH|ENETUNREACH|connection (terminated|closed|reset)|socket hang up|timeout expired)/i.test(msg);
}

async function runSqlIsolated(db, inTx, sql, values) {
  if (inTx) {
    const sp = `sav_${_savepointCounter++}`;
    await db.query(`SAVEPOINT ${sp}`);
    try {
      const qres = await db.query(sql, values);
      await db.query(`RELEASE SAVEPOINT ${sp}`);
      return { ok: true, rowCount: qres.rowCount, rows: qres.rows };
    } catch (err) {
      try {
        await db.query(`ROLLBACK TO SAVEPOINT ${sp}`);
      } catch (rollbackErr) {
        throw err;
      }
      return { ok: false, error: err };
    }
  }
  try {
    const qres = await db.query(sql, values);
    return { ok: true, rowCount: qres.rowCount, rows: qres.rows };
  } catch (err) {
    return { ok: false, error: err };
  }
}

async function runSqlWithTransientRetry({ db, inTx, sql, values, entity, attempts = 3 }) {
  let lastErr;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const res = await runSqlIsolated(db, inTx, sql, values);
    if (res.ok) return res;
    lastErr = res.error;
    if (!isTransientDbError(lastErr)) return res;
    if (attempt < attempts) {
      structuredLog("warn", `[${entity}] Transient DB error, retrying (${attempt}/${attempts})`, {
        error: lastErr.message,
        code: lastErr.code
      });
      await sleep(1000 * attempt);
    }
  }
  return { ok: false, error: lastErr };
}

// Counts how many rows of a successful batch were genuinely NEWLY INSERTED
// (vs already existing and merely updated). Uses the RETURNING marker for
// DO UPDATE upserts and rowCount for DO NOTHING / plain inserts. Purely
// in-memory bookkeeping — no extra queries, no database behavior change.
function countInsertedRows(res, rows) {
  if (res && Array.isArray(res.rows) && res.rows.length > 0 && res.rows[0]["_is_insert"] !== undefined) {
    return res.rows.filter((r) => r["_is_insert"] === true).length;
  }
  if (res && typeof res.rowCount === "number") return res.rowCount;
  return rows.length;
}

async function oneBatchSafe({ db, inTx, buildStatement, table, columns, rows, conflict, updateColumns, entity, idFn, depth = 0 }) {
  const { sql, values } = buildStatement({ table, columns, rows, conflict, updateColumns });
  const res = await runSqlWithTransientRetry({ db, inTx, sql, values, entity });
  if (res.ok) return { synced: rows.length, failed: [], inserted: countInsertedRows(res, rows) };

  if (rows.length === 1) {
    const record = rows[0];
    const identifier = idFn ? idFn(record) : "<unknown>";
    logFailure(entity, identifier, res.error, { table, record: summarizeRecord(record) });
    return { synced: 0, failed: [{ identifier, error: res.error }], inserted: 0 };
  }

  const mid = Math.floor(rows.length / 2);
  const left = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: rows.slice(0, mid), conflict, updateColumns, entity, idFn, depth: depth + 1 });
  const right = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: rows.slice(mid), conflict, updateColumns, entity, idFn, depth: depth + 1 });
  return { synced: left.synced + right.synced, failed: [...left.failed, ...right.failed], inserted: left.inserted + right.inserted };
}

async function writeRowsSafe({ db = pool, inTx = false, table, columns, rows, conflict, updateColumns, batchSize = null, entity = table, idFn, onBatch }) {
  if (!rows || !rows.length) return { synced: 0, failed: [], total: 0, inserted: 0 };
  // Batch size comes from Main Menu -> [2] More Options -> Sync Performance so
  // every writer (vouchers, bills, ledgers, orders, stock) follows one setting.
  const size = clampTuningValue("batchSize", batchSize || (syncTuning && syncTuning.batchSize) || SYNC_TUNING_DEFAULTS.batchSize);
  const useUpsert = !!(conflict && conflict.length);
  const buildStatement = useUpsert ? buildUpsertStatement : buildInsertStatement;
  const total = rows.length;
  let synced = 0;
  let inserted = 0;
  const failed = [];

  for (let i = 0; i < total; i += size) {
    const batch = rows.slice(i, i + size);
    const batchStart = i + 1;
    const batchEnd = Math.min(i + size, total);
    const t0 = Date.now();

    const r = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: batch, conflict, updateColumns, entity, idFn });
    synced += r.synced;
    inserted += r.inserted || 0;
    failed.push(...r.failed);

    structuredLog("info", `[${entity}] SAVE progress: ${synced}/${total} records synced`, {
      saved: synced,
      newRecords: inserted,
      failed: failed.length,
      remaining: total - synced - failed.length,
      batch: `${batchStart}-${batchEnd}`,
      batchMs: Date.now() - t0
    });

    if (onBatch) onBatch({ synced, failed: failed.length, batchEnd, total });
  }

  return { synced, failed, total, inserted };
}

// ============================================================
//  DATABASE INIT (auto-create tables)
// ============================================================
const SCHEMA = `
CREATE TABLE IF NOT EXISTS companies (
  company_guid  TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ledgers (
  company_guid    TEXT NOT NULL,
  ledger_guid     TEXT NOT NULL,
  name            TEXT NOT NULL,
  parent_group    TEXT,
  opening_balance NUMERIC DEFAULT 0,
  closing_balance NUMERIC DEFAULT 0,
  type            TEXT,
  phone           TEXT,
  email           TEXT,
  pan             TEXT,
  updated_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, ledger_guid)
);

CREATE TABLE IF NOT EXISTS vouchers (
  company_guid   TEXT NOT NULL,
  voucher_guid   TEXT NOT NULL,
  voucher_date   DATE,
  voucher_type   TEXT,
  reference_no   TEXT,
  net_amount     NUMERIC DEFAULT 0,
  is_cancelled   BOOLEAN DEFAULT FALSE,
  alter_id       BIGINT,
  master_id      BIGINT,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, voucher_guid)
);
DO $$ BEGIN
  ALTER TABLE vouchers ADD COLUMN is_cancelled BOOLEAN DEFAULT FALSE;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
  ALTER TABLE vouchers ADD COLUMN alter_id BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
  ALTER TABLE vouchers ADD COLUMN master_id BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_vouchers_company_date ON vouchers (company_guid, voucher_date);

CREATE TABLE IF NOT EXISTS invoices (
  company_guid   TEXT NOT NULL,
  invoice_guid   TEXT NOT NULL,
  invoice_no     TEXT,
  invoice_date   DATE,
  invoice_type   TEXT,
  party_name     TEXT,
  total_amount   NUMERIC DEFAULT 0,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, invoice_guid)
);

CREATE TABLE IF NOT EXISTS orders (
  company_guid   TEXT NOT NULL,
  order_guid     TEXT NOT NULL,
  order_no       TEXT,
  order_date     DATE,
  party_name     TEXT,
  total_amount   NUMERIC DEFAULT 0,
  type           TEXT,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, order_guid)
);

CREATE TABLE IF NOT EXISTS bills (
  id             BIGSERIAL PRIMARY KEY,
  company_guid   TEXT NOT NULL,
  ledger_name    TEXT,
  bill_name      TEXT,
  bill_date      DATE,
  bill_amount    NUMERIC DEFAULT 0,
  pending_amount NUMERIC DEFAULT 0,
  due_date       DATE,
  overdue_days   INT DEFAULT 0,
  bill_type      TEXT,
  source_hash    TEXT NOT NULL,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (company_guid, source_hash)
);
-- Migration: bills tables created before updated_at existed must be upgraded,
-- otherwise every bill upsert on an existing row fails.
DO $$ BEGIN
  ALTER TABLE bills ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS inventory_items (
  company_guid   TEXT NOT NULL,
  item_guid      TEXT NOT NULL,
  name           TEXT NOT NULL,
  item_group     TEXT,
  unit           TEXT,
  opening_qty    NUMERIC DEFAULT 0,
  opening_value  NUMERIC DEFAULT 0,
  closing_qty    NUMERIC DEFAULT 0,
  closing_value  NUMERIC DEFAULT 0,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, item_guid)
);

CREATE TABLE IF NOT EXISTS sync_log (
  id             BIGSERIAL PRIMARY KEY,
  company_guid   TEXT,
  company_name   TEXT,
  entity         TEXT,
  records        INT,
  synced_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tracks incremental sync state per entity per company.
-- Replaces the fragile file-based checkpoint for drift detection
-- and "modified since" awareness.
CREATE TABLE IF NOT EXISTS sync_state (
  company_guid   TEXT NOT NULL,
  entity         TEXT NOT NULL,
  last_sync_at   TIMESTAMPTZ,
  last_window    TEXT,
  record_count   INT DEFAULT 0,
  checksum       TEXT,
  meta           JSONB DEFAULT '{}',
  PRIMARY KEY (company_guid, entity)
);
`;

// Add raw JSON payload column to every table (works for existing DBs too)
const PAYLOAD_COLUMNS = `
ALTER TABLE companies ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE ledgers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE vouchers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE bills ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS payload JSONB;
`;

// Obsolete tables that are no longer produced by the agent. Dropped only after
// every code path that referenced them has been removed.
const DROP_OBSOLETE_TABLES = `
DROP TABLE IF EXISTS voucher_entries;
DROP TABLE IF EXISTS inventory_movements;
DROP TABLE IF EXISTS profit_loss;
`;

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(SCHEMA);
    await client.query(PAYLOAD_COLUMNS);
    await client.query(DROP_OBSOLETE_TABLES);
    console.log("✅ Database tables ready");
  } finally {
    client.release();
  }
}

// Generic upsert (duplicate-free) with batching to prevent Postgres parameter overflow
async function upsertRows({ table, columns, rows, conflict, updateColumns, batchSize = null, db = null }) {
  if (!rows || !rows.length) return 0;

  const q = db || pool;
  const size = clampTuningValue("batchSize", batchSize || (syncTuning && syncTuning.batchSize) || SYNC_TUNING_DEFAULTS.batchSize);
  let totalSynced = 0;

  for (let i = 0; i < rows.length; i += size) {
    const batch = rows.slice(i, i + size);
    const { sql, values } = buildUpsertStatement({
      table,
      columns,
      rows: batch,
      conflict,
      updateColumns
    });
    await q.query(sql, values);
    totalSynced += batch.length;
  }

  return totalSynced;
}

// Delete records for a company that no longer exist in Tally (uses single ANY array parameter)
async function cleanupByGuids(table, companyGuid, guidColumn, guids, db = null) {
  if (!guids || !guids.length) return;
  const q = db || pool;
  const sql = `DELETE FROM "${table}" WHERE company_guid = $1 AND NOT (${guidColumn} = ANY($2::text[]))`;
  const res = await q.query(sql, [companyGuid, guids]);
  devLog(`🧹 Cleanup ${table}: removed ${res.rowCount} missing records`);
}

function logSync(entity, records) {
  return pool.query(
    "INSERT INTO sync_log (company_guid, company_name, entity, records) VALUES ($1, $2, $3, $4)",
    [ACTIVE_COMPANY_GUID, ACTIVE_COMPANY_NAME, entity, records]
  );
}

// ============================================================
//  1. DETECT + CREATE COMPANIES
// ============================================================
const SHORT_MONTHS = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12
};

// Tally company names look like "Acme Ltd - (From 1-Apr-2016)".
function parseStartingFromName(name) {
  const m = /\(From\s+(\d{1,2})-([A-Za-z]+)-(\d{4})\)/i.exec(name || "");
  if (!m) return null;
  const mon = SHORT_MONTHS[(m[2] || "").toLowerCase().slice(0, 3)];
  if (!mon) return null;
  const day = parseInt(m[1], 10);
  const year = parseInt(m[3], 10);
  if (year < 1900 || year > 3000) return null;
  return `${year}-${String(mon).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

// Normalizes Tally date strings ("01-04-2016", "1-Apr-2016", "20160401") to ISO.
function normalizeTallyDate(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (m) return s;
  m = /^(\d{8})$/.exec(s);
  if (m) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  m = /^(\d{1,2})-(\d{1,2})-(\d{4})$/.exec(s);
  if (m) {
    const day = parseInt(m[1], 10);
    const mon = parseInt(m[2], 10);
    if (mon >= 1 && mon <= 12 && day >= 1 && day <= 31) {
      return `${parseInt(m[3], 10)}-${String(mon).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
  }
  m = /^(\d{1,2})-([A-Za-z]+)-(\d{4})$/.exec(s);
  if (m) {
    const mon = SHORT_MONTHS[(m[2] || "").toLowerCase().slice(0, 3)];
    if (mon) {
      return `${parseInt(m[3], 10)}-${String(mon).padStart(2, "0")}-${String(parseInt(m[1], 10)).padStart(2, "0")}`;
    }
  }
  return null;
}

// Best-effort fetch of each company's "Starting From" date via a FIELD-based
// collection. If Tally rejects the request it returns a LINEERROR (never hangs);
// we simply fall back to name parsing / env SYNC_START_YEAR.
async function fetchCompanyStartDates() {
  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Company Starting Dates</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   </STATICVARIABLES>
    <TDL>
     <TDLMESSAGE>
      <COLLECTION NAME="Company Starting Dates">
       <TYPE>Company</TYPE>
       <NATIVEMETHOD>Name</NATIVEMETHOD>
       <NATIVEMETHOD>GUID</NATIVEMETHOD>
       <NATIVEMETHOD>StartingFrom</NATIVEMETHOD>
      </COLLECTION>
     </TDLMESSAGE>
    </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  try {
    const response = await fetchTallyXml(xmlRequest, { entity: "company-dates" });
    if (!response.data || response.data.includes("<LINEERROR>")) return {};
    const jsonData = await parseStringPromise(response.data);
    const companies =
      jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];
    const map = {};
    for (const c of companies) {
      const guid = typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;
      if (!guid) continue;
      let raw = c.STARTINGFROM;
      if (Array.isArray(raw)) raw = raw[0];
      if (raw && typeof raw === "object") raw = raw._;
      map[guid] = raw ? normalizeTallyDate(String(raw)) : null;
    }
    structuredLog("info", `[companies] Starting From dates fetched: ${Object.entries(map).filter(([, v]) => v).length}/${Object.keys(map).length} companies`);
    return map;
  } catch (err) {
    structuredLog("warn", "[companies] Could not fetch Starting From dates", {
      error: err.message
    });
    return {};
  }
}

async function detectCompanies() {
  const t0 = Date.now();

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Company Collection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="Company Collection">
      <TYPE>Company</TYPE>
      <FETCH>NAME,GUID</FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const response = await fetchTallyXml(xmlRequest, { entity: "companies" });

  const jsonData = await parseStringPromise(response.data);

  const companies =
    jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];

  const companyList = [];
  let skipped = 0;

  for (const c of companies) {
    const company_guid =
      typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;
    const name =
      typeof c.NAME?.[0] === "string" ? c.NAME[0] : c.NAME?.[0]?._;

    if (!company_guid || !name) {
      skipped++;
      continue;
    }

    const startingFrom = parseStartingFromName(name);
    companyList.push({ company_guid, name, startingFrom, payload: c });
  }

  structuredLog("info", `[companies] VALIDATE: ${companies.length} fetched, ${skipped} skipped, ${companyList.length} valid`);
  TALLY_OPEN_COMPANY_COUNT = companyList.length;

  // Best-effort: enrich with the real "Starting From" date reported by Tally
  // (companies without a "(From ...)" suffix in their name rely on this).
  const startDates = await fetchCompanyStartDates();
  for (const c of companyList) {
    if (!c.startingFrom && startDates[c.company_guid]) {
      c.startingFrom = startDates[c.company_guid];
    }
  }
  const withStartDate = companyList.filter((c) => c.startingFrom).length;
  structuredLog("info", `[companies] starting dates resolved for ${withStartDate}/${companyList.length} companies`);

  // 🔥 Auto-create companies in DB (duplicate-free upsert)
  const res = await writeRowsSafe({
    table: "companies",
    columns: ["company_guid", "name", "payload"],
    rows: companyList,
    conflict: ["company_guid"],
    updateColumns: ["name", "payload"],
    entity: "companies",
    idFn: (r) => r.company_guid
  });

  structuredLog("info", `[companies] COMPLETE: fetched=${companies.length} saved=${res.synced} failed=${res.failed.length} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });

  console.log(
    "🏢 Companies detected from Tally:",
    companyList.map((c) => c.name).join(", ")
  );

  return companyList;
}

// ============================================================
//  2. SYNC LEDGERS
// ============================================================
async function syncLedgers(company) {
  const t0 = Date.now();
  progressStart("Fetching Ledgers");

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Ledger Collection</ID>
 </HEADER>
 <BODY>
  <DESC>
<STATICVARIABLES>
  <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
  <SVCURRENTCOMPANY>${company.name}</SVCURRENTCOMPANY>
</STATICVARIABLES>

   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="Ledger Collection">
      <TYPE>Ledger</TYPE>
<FETCH>
  NAME,
  GUID,
  PARENT,
  ADDRESS,
  ADDRESS.LIST,
  MAILINGDETAILS.LIST,
  OPENINGBALANCE,
  CLOSINGBALANCE,
  ISBILLWISEON,
  LEDGERPHONE,
  EMAIL,
  INCOMETAXNUMBER
</FETCH>

     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xmlRequest, { entity: "ledgers" });

  const parser = new xml2js.Parser({ explicitArray: false });
  const parsed = await parser.parseStringPromise(res.data);

  const ledgers = parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.LEDGER || [];
  const ledgerArray = Array.isArray(ledgers) ? ledgers : [ledgers];

  devLog("Ledgers from Tally:", ledgerArray.length);
  progressUpdate(ledgerArray.length);

  const ledgerRows = [];
  const ledgerGuids = [];
  let skipped = 0;

  for (const l of ledgerArray) {
    let mailingAddressText = null;
    const mailingRaw = l["MAILINGDETAILS.LIST"];
    const mailingArr = Array.isArray(mailingRaw)
      ? mailingRaw
      : mailingRaw
      ? [mailingRaw]
      : [];

    for (const m of mailingArr) {
      if (m?.["ADDRESS.LIST"]?.ADDRESS) {
        mailingAddressText = extractAddressValue(m["ADDRESS.LIST"].ADDRESS);
        break;
      }
    }

    const ledgerName =
      l.$?.NAME ||
      l.NAME ||
      (typeof l.PARENT === "string" ? l.PARENT : null);

    if (!ledgerName) {
      devLog("⚠️ Skipping ledger without name");
      skipped++;
      continue;
    }

    const rawPhone = l.LEDGERPHONE?._ || l.LEDGERPHONE || null;
    const rawEmail = l.EMAIL?._ || l.EMAIL || null;
    const addressText = normalizeText(l.ADDRESS);
    let addressListText = null;
    if (l["ADDRESS.LIST"]?.ADDRESS) {
      addressListText = extractAddressValue(l["ADDRESS.LIST"].ADDRESS);
    }

    const searchBlob = [
      rawPhone,
      rawEmail,
      addressText,
      addressListText,
      mailingAddressText,
      l.NAME,
      l.PARENT
    ]
      .filter(Boolean)
      .join(" ");

    const phone = extractPhone(searchBlob);
    const email = extractEmail(searchBlob);

    const pan =
      typeof l.INCOMETAXNUMBER === "string"
        ? l.INCOMETAXNUMBER
        : l.INCOMETAXNUMBER?._ || null;

    const ledgerGuid =
      typeof l.GUID === "string" ? l.GUID : l.GUID?._ || null;

    if (!ledgerGuid || !ledgerName) {
      devLog("⏭️ Skipping invalid ledger:", ledgerName);
      skipped++;
      continue;
    }

    ledgerGuids.push(ledgerGuid);

    ledgerRows.push({
      company_guid: company.company_guid,
      ledger_guid: ledgerGuid,
      name: ledgerName,
      parent_group: typeof l.PARENT === "string" ? l.PARENT : l.PARENT?._,
      opening_balance: parseTallyNumber(l.OPENINGBALANCE?._ || "0"),
      closing_balance: parseTallyNumber(l.CLOSINGBALANCE?._ || "0"),
      type: l.ISBILLWISEON === "Yes" ? "Party" : "General",
      phone,
      email,
      pan,
      payload: l
    });
  }

  structuredLog("info", `[ledgers] VALIDATE: ${ledgerArray.length} fetched, ${skipped} skipped, ${ledgerRows.length} valid`);

  const result = await writeRowsSafe({
    table: "ledgers",
    columns: [
      "company_guid",
      "ledger_guid",
      "name",
      "parent_group",
      "opening_balance",
      "closing_balance",
      "type",
      "phone",
      "email",
      "pan",
      "payload"
    ],
    rows: ledgerRows,
    conflict: ["company_guid", "ledger_guid"],
    updateColumns: [
      "name",
      "parent_group",
      "opening_balance",
      "closing_balance",
      "type",
      "phone",
      "email",
      "pan",
      "payload"
    ],
    entity: "ledgers",
    idFn: (r) => r.ledger_guid
  });

  await cleanupByGuids("ledgers", company.company_guid, "ledger_guid", ledgerGuids);
  await logSync("ledgers", result.synced);
  progressEnd(ledgerRows.length, "Ledgers");

  const ledgerChecksum = computeGuidChecksum(ledgerGuids);
  const prevLedgerState = await getSyncState(ACTIVE_COMPANY_GUID, "ledgers");
  const ledgerChanged = prevLedgerState ? prevLedgerState.checksum !== ledgerChecksum : true;

  recordEntityStats("ledgers", {
    fetched: ledgerArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[ledgers] COMPLETE: fetched=${ledgerArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${ledgerChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const ledgerDbCount = await getDbCount("ledgers", ACTIVE_COMPANY_GUID);
  devLog(`✅ Ledgers: ${result.synced} new (${ledgerDbCount} in DB)${ledgerChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "ledgers", {
      lastSyncAt: new Date().toISOString(),
      recordCount: ledgerArray.length,
      checksum: ledgerChecksum,
      meta: { changed: ledgerChanged }
    });
  } catch {}
}

// Helper: get DB count for any entity table
async function getDbCount(table, companyGuid, extraWhere = "", extraParams = []) {
  try {
    const where = extraWhere ? `AND ${extraWhere}` : "";
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM ${table} WHERE company_guid = $1 ${where}`,
      [companyGuid, ...extraParams]
    );
    return rows[0]?.n ?? 0;
  } catch { return 0; }
}

// ============================================================
//  3. SYNC VOUCHERS (+ INVOICES + ENTRIES)
// ============================================================
async function syncVouchers(company, opts = {}) {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();

  // Invoices are derived from the same "Voucher Register" response as vouchers,
  // so either selection triggers the (single) Tally request; only the selected
  // outputs are written to the database. Defaults keep old callers syncing both.
  const saveVouchers = opts.saveVouchers !== false;
  const saveInvoices = opts.saveInvoices !== false;

  // Live single-line progress for the shared Voucher Register fetch.
  const voucherProgressLabel =
    saveVouchers && saveInvoices
      ? "Fetching Vouchers & Invoices"
      : saveVouchers
      ? "Fetching Vouchers"
      : "Fetching Invoices";
  progressStart(voucherProgressLabel);

  // Smaller windows = smaller Tally responses = each window finishes fast and
  // checkpoints stay fine-grained on huge companies. Configured from
  // Main Menu -> [2] More Options -> Sync Performance (or the VOUCHER_WINDOW_DAYS
  // environment variable, which only supplies the default).
  const VOUCHER_WINDOW_DAYS = syncTuning.windowDays;
  // Records read from the Tally stream / written to the database per batch.
  const SYNC_BATCH_SIZE = syncTuning.batchSize;
  // Voucher windows must never wait forever (default TALLY_TIMEOUT_MS is 0).
  // This is an IDLE timeout: it only fires when Tally sends nothing for this
  // long, so a big response that keeps streaming is never cut off. 10 minutes
  // leaves room for Tally to build a large report before its first byte.
  // Override with VOUCHER_STREAM_TIMEOUT_MS (ms).
  const VOUCHER_STREAM_TIMEOUT_MS = Number(process.env.VOUCHER_STREAM_TIMEOUT_MS) || 600000;

  // Memory visibility: the heap limit actually in effect (confirms whether the
  // 4 GB setting reached the EXE) and the current usage. WINDOW DONE lines
  // also carry mem=, so any growth before a crash is visible in the log.
  try {
    const hs = require("v8").getHeapStatistics();
    structuredLog("info", `[vouchers] MEMORY: heap limit ${fmtBytes(hs.heap_size_limit)}, in use ${fmtBytes(process.memoryUsage().rss)}`);
  } catch {}
  structuredLog("info", `[vouchers] TUNING: windowDays=${VOUCHER_WINDOW_DAYS} batchSize=${SYNC_BATCH_SIZE} recentWindowDays=${syncTuning.recentWindowDays}`);

  // Several companies open in Tally => the Voucher Register can ignore the
  // date range (every window becomes a full-book export) and may serve the
  // company that is active in Tally's UI instead of SVCURRENTCOMPANY.
  if (TALLY_OPEN_COMPANY_COUNT > 1) {
    const msg = `[vouchers] WARNING: ${TALLY_OPEN_COMPANY_COUNT} companies are open in Tally. For large companies keep ONLY "${ACTIVE_COMPANY_NAME}" open during voucher sync — with several open, Tally may ignore the date windows (very slow full-book exports) and may return the company that is active in Tally's screen.`;
    structuredLog("warn", msg);
    progressBreak();
    console.log(`⚠️  ${TALLY_OPEN_COMPANY_COUNT} companies are open in Tally. For best results keep only "${ACTIVE_COMPANY_NAME}" open while vouchers sync.`);
  }
  const { startYear: syncStartYear, startMonth: syncStartMonth } = resolveSyncPeriod(company);
  const chunks = getVoucherWindows(new Date(syncStartYear, syncStartMonth - 1, 1), VOUCHER_WINDOW_DAYS);

  // Neither a selective-sync setting nor a company "starting from" date is
  // available, so the window grid starts at the 1900 fallback. That is
  // thousands of requests to Tally in a row (mostly returning nothing), which
  // is exactly the load pattern that wedges Tally Prime — and it takes days to
  // finish. Say so instead of quietly hammering the server.
  if (syncStartYear <= 1900) {
    const msg = `[vouchers] WARNING: no start period could be detected for "${ACTIVE_COMPANY_NAME}" (Tally did not report a "Starting From" date), so the scan starts at 1900 — about ${chunks.length.toLocaleString()} Tally requests. Set Main Menu -> [4] Sync Settings to the real company start period to avoid this.`;
    structuredLog("warn", msg);
    progressBreak();
    console.log(`⚠️  No start period detected for "${ACTIVE_COMPANY_NAME}" — the voucher scan falls back to 1900 (${chunks.length.toLocaleString()} requests to Tally).`);
    console.log("   Set the real start period in [4] Sync Settings to make this fast and easy on Tally.\n");
  }

  const checkpoint = getCompanyCheckpoint(ACTIVE_COMPANY_GUID).vouchers || {};
  let lastWindow =
    checkpoint.startYear === syncStartYear && checkpoint.startMonth === syncStartMonth
      ? checkpoint.lastWindow || null
      : null;

  // COUNT INTEGRITY. The resume checkpoint must mean "every window up to here
  // succeeded". Older builds advanced it past FAILED windows, so a later resume
  // skipped those periods forever (the missing-voucher count). Checkpoints
  // written by this build are contiguous (contiguousV = 2). Any older
  // checkpoint may hide gaps, so it is discarded ONCE and every window is
  // re-scanned; existing rows are upserted by GUID, never duplicated.
  const VOUCHER_CHECKPOINT_VERSION = 2;
  const sameCheckpointPeriod =
    checkpoint.startYear === syncStartYear && checkpoint.startMonth === syncStartMonth;
  if (lastWindow && checkpoint.contiguousV !== VOUCHER_CHECKPOINT_VERSION) {
    structuredLog("warn", `[vouchers] RESCAN: checkpoint ${lastWindow} was written by an older build and may skip failed periods — re-scanning all windows once to repair missing vouchers`);
    progressBreak();
    console.log("🔁 One-time full voucher re-scan to repair any missing periods (existing records are updated, not duplicated).");
    lastWindow = null;
  }
  // Safety net: a complete full re-scan every FULL_RESCAN_HOURS (default 24,
  // 0 = off) so anything missed for any reason is picked up within a day.
  // Only forced after a full pass has COMPLETED; an interrupted pass resumes
  // from its checkpoint instead of restarting.
  const FULL_RESCAN_HOURS =
    process.env.FULL_RESCAN_HOURS !== undefined && process.env.FULL_RESCAN_HOURS !== ""
      ? Number(process.env.FULL_RESCAN_HOURS)
      : 24;
  if (
    lastWindow &&
    FULL_RESCAN_HOURS > 0 &&
    checkpoint.fullPassAt &&
    Date.now() - new Date(checkpoint.fullPassAt).getTime() > FULL_RESCAN_HOURS * 3600000
  ) {
    structuredLog("info", `[vouchers] RESCAN: last complete full pass was ${checkpoint.fullPassAt} (> ${FULL_RESCAN_HOURS}h ago) — re-scanning all windows`);
    lastWindow = null;
  }
  // Remember when the current full pass began, so its completion can be
  // stamped even if it spans several runs (pause / crash / Tally busy).
  let passStartedAt =
    !lastWindow || !sameCheckpointPeriod || !checkpoint.passStartedAt
      ? new Date().toISOString()
      : checkpoint.passStartedAt;

  // Grid change: a different window width produces differently sized/aligned
  // windows, so a resume point saved on the old grid can silently skip periods
  // that were never fetched on the new one. Drop the checkpoint and re-scan the
  // new grid once — rows are upserted by GUID, so nothing is duplicated.
  if (lastWindow && checkpoint.windowDays !== VOUCHER_WINDOW_DAYS) {
    structuredLog("info", `[vouchers] RESCAN: window size changed (${checkpoint.windowDays ?? "unknown"}d -> ${VOUCHER_WINDOW_DAYS}d) — re-scanning on the new window grid`);
    progressBreak();
    console.log(`🔁 Sync window size changed to ${VOUCHER_WINDOW_DAYS} day(s) — windows are re-scanned on the new grid (existing records are updated, not duplicated).`);
    lastWindow = null;
    passStartedAt = new Date().toISOString();
  }

  // Self-heal: if the DB holds no rows for the selected output (table cleared,
  // DB restored, etc.), the resume checkpoint is stale — it would skip every
  // window and leave the data permanently empty. Ignore the checkpoint and do
  // a full re-sync so the rows get fetched again.
  if (lastWindow) {
    try {
      const outputTable = saveVouchers ? "vouchers" : "invoices";
      const { rows } = await pool.query(
        `SELECT count(*)::int AS n FROM ${outputTable} WHERE company_guid = $1`,
        [ACTIVE_COMPANY_GUID]
      );
      if (!rows[0] || rows[0].n === 0) {
        structuredLog("warn", `[vouchers] DB has no ${outputTable} rows, ignoring checkpoint and re-syncing all windows`);
        lastWindow = null;
        passStartedAt = new Date().toISOString();
      }
    } catch (err) {
      structuredLog("warn", "[vouchers] could not verify output count, keeping checkpoint", {
        error: err.message
      });
    }
  }

  // ⚠️ BOUNDED REQUEST: "Voucher Register" REPORT, NOT a bare VoucherCollection.
  // A bare Voucher collection ignores SVFROMDATE/SVTODATE and returns the WHOLE
  // company (current financial year) in one shot — on large companies that makes
  // Tally Prime run out of memory ("memory excess violation") and hang. The
  // register report honours the date range, so each window stays small.
  // Cancelled vouchers are filtered client-side (ISDELETED / ISCANCELLED check).
  const xmlTemplate = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVFROMDATE>__FROM_DATE__</SVFROMDATE>
     <SVTODATE>__TO_DATE__</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;
  let skipped = 0;
  let cancelledCount = 0;
  // Raw rows returned by Tally (each voucher appears multiple times in the
  // Voucher Register XML). Kept for diagnostic logs only — the summary reports
  // deduplicated counts instead.
  let totalFetched = 0;
  // Unique invoice-eligible vouchers (sales/purchase) actually processed.
  let invoiceCandidates = 0;
  // Unconfirmed draft vouchers (optional / post-dated). Tally Prime does not
  // count these in its on-screen voucher totals until they are confirmed, so
  // the breakdown excludes them too (they are still stored in the DB).
  let draftVouchers = 0;
  let draftCancelled = 0;
  const draftList = [];
  const voucherTypeCounts = {};
  const cancelledTypeCounts = {};
  // Confirmed (non-optional) vouchers per type — feeds the breakdown "Total"
  // column so it matches Tally Prime's Types-of-Vouchers count exactly.
  const confirmedTypeCounts = {};
  // Deduplicate: Voucher Register XML returns each voucher multiple times
  // (once per voucher-type grouping). Without dedup the same cancelled voucher
  // is counted N times, inflating the cancelled total.
  const countedGuids = new Set();
  const stats = {
    voucherSaved: 0,
    voucherFailed: 0,
    voucherInserted: 0,
    invoiceSaved: 0,
    invoiceFailed: 0,
    invoiceInserted: 0
  };

  function extractValue(val) {
    if (!val) return null;
    if (typeof val === "string") return val;
    if (typeof val === "object" && val._) return val._;
    return null;
  }

  function processVoucher(v, rows) {
    const voucherGuid = extractValue(v.GUID?.[0]);
    if (!voucherGuid) {
      skipped++;
      return;
    }

    // Deduplicate: Voucher Register XML repeats each voucher ~N times (once per
    // type grouping). We only count / process each GUID ONCE.
    if (countedGuids.has(voucherGuid)) return;
    countedGuids.add(voucherGuid);

    // Count cancelled / deleted vouchers separately — still save them with is_cancelled flag
    const isDeleted = v.ISDELETED ? v.ISDELETED[0] : null;
    const isCancelled = v.ISCANCELLED ? v.ISCANCELLED[0] : null;
    const voucherIsCancelled = isDeleted === "Yes" || isCancelled === "Yes";
    // Tally's "Cancelled" count uses only ISCANCELLED (not ISDELETED).
    // ISDELETED means the voucher was deleted — Tally still shows it in the
    // register and counts it as active, not cancelled.
    const tallyCancelled = extractValue(v.ISCANCELLED?.[0]) === "Yes";
    const isOptional = extractValue(v.ISOPTIONAL?.[0]) === "Yes";
    if (tallyCancelled) {
      cancelledCount++;
      const rawType = extractValue(v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]);
      const cTypeKey = (rawType || "").trim() || "Unknown";
      // The "Cancelled" count mirrors Tally Prime's Types-of-Vouchers report:
      // every cancelled voucher is counted, including optional/draft ones.
      cancelledTypeCounts[cTypeKey] = (cancelledTypeCounts[cTypeKey] || 0) + 1;
      if (!isOptional) {
        // Confirmed (non-optional) vouchers appear in the breakdown Total.
        confirmedTypeCounts[cTypeKey] = (confirmedTypeCounts[cTypeKey] || 0) + 1;
      }
      if (isOptional) {
        draftCancelled++;
      }
    }

    rows.chunkGuids.push(voucherGuid);

    const rawDate = extractValue(v.DATE?.[0] || v.REFERENCEDATE?.[0]);
    const voucherDate = toISODate(rawDate);
    const voucherType = extractValue(v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]);
    const vTypeKey = (voucherType || "").trim() || "Unknown";
    if (!tallyCancelled) {
      if (isOptional) {
        draftVouchers++;
      } else {
        voucherTypeCounts[vTypeKey] = (voucherTypeCounts[vTypeKey] || 0) + 1;
        confirmedTypeCounts[vTypeKey] = (confirmedTypeCounts[vTypeKey] || 0) + 1;
      }
    }
    const referenceNo =
      extractValue(v.REFERENCE?.[0] || v.REFERENCENUMBER?.[0] || v.VOUCHERNUMBER?.[0]);

    if (isOptional) {
      draftList.push({
        reference_no: referenceNo || extractValue(v.VOUCHERNUMBER?.[0]),
        voucher_number: extractValue(v.VOUCHERNUMBER?.[0]),
        voucher_type: vTypeKey,
        voucher_date: voucherDate,
        narration: extractValue(v.NARRATION?.[0]),
        cancelled: tallyCancelled
      });
    }

    const rawLedgerEntries = [
      ...(Array.isArray(v["ALLLEDGERENTRIES.LIST"])
        ? v["ALLLEDGERENTRIES.LIST"]
        : v["ALLLEDGERENTRIES.LIST"]
        ? [v["ALLLEDGERENTRIES.LIST"]]
        : []),
      ...(Array.isArray(v["LEDGERENTRIES.LIST"])
        ? v["LEDGERENTRIES.LIST"]
        : v["LEDGERENTRIES.LIST"]
        ? [v["LEDGERENTRIES.LIST"]]
        : [])
    ];

    const entryArray = Array.isArray(rawLedgerEntries)
      ? rawLedgerEntries.filter((e) => typeof e === "object")
      : typeof rawLedgerEntries === "object"
      ? [rawLedgerEntries]
      : [];

    const entries = [];

    for (const e of entryArray) {
      const ledgerName = extractValue(e.LEDGERNAME?.[0]);
      const amountRaw = extractValue(e.AMOUNT?.[0]);
      if (!ledgerName || !amountRaw) continue;

      const isDebit = extractValue(e.ISDEEMEDPOSITIVE?.[0]) === "No";

      entries.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        ledger_name: ledgerName,
        amount: Math.abs(Number(amountRaw)),
        is_debit: isDebit,
        payload: e
      });
    }

    if (entries.length === 0 && v.PARTYLEDGERNAME?.[0]) {
      entries.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        ledger_name: extractValue(v.PARTYLEDGERNAME?.[0]),
        amount: 0,
        is_debit: false,
        payload: v
      });
    }

    let debitTotal = 0;
    let creditTotal = 0;
    for (const entry of entries) {
      if (entry.is_debit) debitTotal += entry.amount;
      else creditTotal += entry.amount;
    }

    const netAmount = Math.max(debitTotal, creditTotal);

    const alterIdRaw = extractValue(v.ALTERID?.[0] || v.ALTERID);
    const masterIdRaw = extractValue(v.MASTERID?.[0] || v.MASTERID);
    const alterId = alterIdRaw ? parseTallyNumber(alterIdRaw) || null : null;
    const masterId = masterIdRaw ? parseTallyNumber(masterIdRaw) || null : null;

    if (saveVouchers) {
      rows.voucherRows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        voucher_date: voucherDate,
        voucher_type: voucherType,
        reference_no: referenceNo,
        net_amount: netAmount,
        is_cancelled: voucherIsCancelled,
        alter_id: alterId,
        master_id: masterId,
        payload: v
      });
    }

    // Order vouchers (Sales Order / Purchase Order) contain "sales"/"purchase"
    // in their names but are NOT invoices — they belong to the orders entity.
    const tLower = (voucherType || "").toLowerCase();
    const isOrderType = tLower.includes("order");
    const isPurchase =
      voucherType && !isOrderType && tLower.includes("purchase");
    const isSales =
      voucherType &&
      !isOrderType &&
      (tLower.includes("sales") || tLower.includes("invoice"));

    if (saveInvoices && !voucherIsCancelled && (isPurchase || isSales)) {
      invoiceCandidates++;
      rows.invoiceRows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        invoice_guid: voucherGuid,
        invoice_no: extractValue(v.VOUCHERNUMBER?.[0]),
        invoice_date: voucherDate,
        invoice_type: isPurchase ? "Purchase" : "Sales",
        party_name: extractValue(v.PARTYLEDGERNAME?.[0]),
        total_amount: netAmount,
        payload: v
      });
    }
  }

  async function writeChunk(rows) {
    if (!rows.chunkGuids.length) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const voucherRes = saveVouchers
        ? await writeRowsSafe({
            db: client,
            inTx: true,
            table: "vouchers",
            columns: [
              "company_guid",
              "voucher_guid",
              "voucher_date",
              "voucher_type",
              "reference_no",
              "net_amount",
              "is_cancelled",
              "alter_id",
              "master_id",
              "payload"
            ],
            rows: rows.voucherRows,
            conflict: ["company_guid", "voucher_guid"],
            updateColumns: [
              "voucher_date",
              "voucher_type",
              "reference_no",
              "net_amount",
              "is_cancelled",
              "alter_id",
              "master_id",
              "payload"
            ],
            entity: "vouchers",
            idFn: (r) => r.voucher_guid
          })
        : { synced: 0, failed: [] };

      const invoiceRes = saveInvoices
        ? await writeRowsSafe({
            db: client,
            inTx: true,
            table: "invoices",
            columns: [
              "company_guid",
              "invoice_guid",
              "invoice_no",
              "invoice_date",
              "invoice_type",
              "party_name",
              "total_amount",
              "payload"
            ],
            rows: rows.invoiceRows,
            conflict: ["company_guid", "invoice_guid"],
            updateColumns: [
              "invoice_no",
              "invoice_date",
              "invoice_type",
              "party_name",
              "total_amount",
              "payload"
            ],
            entity: "invoices",
            idFn: (r) => r.invoice_guid
          })
        : { synced: 0, failed: [] };

      await client.query("COMMIT");

      stats.voucherSaved += voucherRes.synced;
      stats.voucherFailed += voucherRes.failed.length;
      stats.voucherInserted += voucherRes.inserted || 0;
      stats.invoiceSaved += invoiceRes.synced;
      stats.invoiceFailed += invoiceRes.failed.length;
      stats.invoiceInserted += invoiceRes.inserted || 0;
    } catch (err) {
      await client.query("ROLLBACK");
      structuredLog("error", "[vouchers] Batch sync failed, transaction rolled back", {
        error: err.message
      });
      throw err;
    } finally {
      client.release();
    }
  }

  // Removes vouchers that Tally no longer reports for this window (bounded to the
  // window's own date range, so memory stays flat regardless of company size).
  // Only runs when the window produced at least one voucher, so an empty Tally
  // response (e.g. wrong active date) can never wipe existing data.
  async function cleanupWindow(fromDate, toDate, windowGuids) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      let removed = [];
      if (saveVouchers) {
        const delRes = await client.query(
          `DELETE FROM vouchers
            WHERE company_guid = $1
              AND voucher_date >= $2::date
              AND voucher_date <= $3::date
              AND NOT (voucher_guid = ANY($4::text[]))
            RETURNING voucher_guid`,
          [ACTIVE_COMPANY_GUID, fromDate, toDate, windowGuids]
        );
        removed = delRes.rows.map((r) => r.voucher_guid);
      }
      if (removed.length && saveInvoices) {
        await client.query(
          "DELETE FROM invoices WHERE company_guid = $1 AND invoice_guid = ANY($2::text[])",
          [ACTIVE_COMPANY_GUID, removed]
        );
      }
      await client.query("COMMIT");
      structuredLog("info", `[vouchers] WINDOW CLEANUP: ${fromDate}-${toDate} removed ${removed.length} stale vouchers`);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async function getVoucherAlterIds(companyGuid, from, to) {
    const fStr = typeof from === "string" ? from : fmtYMD(from);
    const tStr = typeof to === "string" ? to : fmtYMD(to);
    const fromIso = `${fStr.slice(0, 4)}-${fStr.slice(4, 6)}-${fStr.slice(6, 8)}`;
    const toIso = `${tStr.slice(0, 4)}-${tStr.slice(4, 6)}-${tStr.slice(6, 8)}`;
    const { rows } = await pool.query(
      `SELECT voucher_guid, alter_id::text AS alter_id FROM vouchers
       WHERE company_guid = $1 AND voucher_date >= $2::date AND voucher_date <= $3::date`,
      [companyGuid, fromIso, toIso]
    );
    const map = new Map();
    for (const r of rows) {
      map.set(r.voucher_guid, r.alter_id ? String(r.alter_id) : null);
    }
    return map;
  }

  async function markVouchersDeleted(companyGuid, guids) {
    if (!guids || !guids.length) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      if (saveVouchers) {
        await client.query(
          `DELETE FROM vouchers WHERE company_guid = $1 AND voucher_guid = ANY($2::text[])`,
          [companyGuid, guids]
        );
      }
      if (saveInvoices) {
        await client.query(
          `DELETE FROM invoices WHERE company_guid = $1 AND invoice_guid = ANY($2::text[])`,
          [companyGuid, guids]
        );
      }
      await client.query("COMMIT");
      structuredLog("info", `[vouchers] REMOVED STALE VOUCHERS: ${guids.length} removed`);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async function saveVouchersFromStream(streamCtx, res) {
    const parser = saxVoucherStream();
    const decoder = new StringDecoder("utf8");
    let entityCarry = "";
    let pending = [];
    let pendingCount = 0;
    let flushing = false;
    let ended = false;
    let bytes = 0;

    async function drain() {
      while (pending.length) {
        const items = pending;
        pending = [];
        pendingCount = 0;
        const batch = { voucherRows: [], invoiceRows: [], chunkGuids: [] };
        for (const v of items) {
          totalFetched++;
          processVoucher(v, batch);
        }
        if (batch.chunkGuids.length) {
          await writeChunk(batch);
        }
        progressUpdate(countedGuids.size);
      }
    }

    function feed(text, final) {
      let t = entityCarry + text;
      entityCarry = "";
      if (!final) {
        const lastAmp = t.lastIndexOf("&");
        if (lastAmp !== -1 && t.indexOf(";", lastAmp) === -1 && t.length - lastAmp <= 12) {
          entityCarry = t.slice(lastAmp);
          t = t.slice(0, lastAmp);
        }
      }
      if (t) parser.write(sanitizeXml(t));
    }

    await new Promise((resolve, reject) => {
      let settled = false;
      const done = (fn, v) => { if (!settled) { settled = true; fn(v); } };

      res.on("data", (chunk) => {
        res.pause();
        bytes += chunk.length;
        try {
          feed(decoder.write(chunk), false);
          for (const v of parser.consumeDrained()) {
            pending.push(v);
            pendingCount++;
          }
          if (pendingCount >= SYNC_BATCH_SIZE && !flushing) {
            flushing = true;
            drain()
              .catch((e) => {
                if (!res.destroyed) res.destroy();
                done(reject, e);
              })
              .finally(() => {
                flushing = false;
                if (!res.destroyed) res.resume();
              });
          } else if (!res.destroyed) {
            res.resume();
          }
        } catch (e) {
          if (!res.destroyed) res.destroy();
          done(reject, e);
        }
      });

      res.on("end", async () => {
        ended = true;
        try {
          feed(decoder.end(), true);
          parser.close();
          for (const v of parser.consumeDrained()) pending.push(v);
          await drain();
          done(resolve);
        } catch (e) {
          done(reject, e);
        }
      });

      res.on("error", (err) => done(reject, err));
      res.on("close", () => {
        if (!ended) {
          done(reject, new Error("Tally connection closed before response completed"));
        }
      });
    });

    if (parser.seenLineError) {
      throw new TallyLogicalError("Tally returned LINEERROR (report request failed)");
    }
    return { bytes };
  }

  // updateCheckpoint=false is used for future-dated voucher windows: the resume
  // checkpoint tracks progress through the past/today windows only, so it must
  // never jump ahead to a future date (that would skip unfinished past windows).
  async function processDateRange(fromDate, toDate, { updateCheckpoint = true } = {}) {
    const xmlRequest = xmlTemplate
      .replace("__FROM_DATE__", fromDate)
      .replace("__TO_DATE__", toDate);

    const windowGuids = [];
    let fetchedInWindow = 0;
    // Full-book detection (working-agent logic): if Tally returns vouchers
    // dated OUTSIDE the requested window, it ignored SVFROMDATE/SVTODATE and
    // sent the entire book (happens when several companies are open in Tally).
    let outOfRange = 0;
    let minDate = null;
    let maxDate = null;

    const onBatch = async (items) => {
      const batch = { voucherRows: [], invoiceRows: [], chunkGuids: [] };
      for (const v of items) {
        fetchedInWindow++;
        totalFetched++;
        const d = extractValue(v.DATE?.[0]);
        if (d && /^\d{8}$/.test(d)) {
          if (d < fromDate || d > toDate) outOfRange++;
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
        processVoucher(v, batch);
      }
      if (batch.chunkGuids.length) {
        await writeChunk(batch);
        windowGuids.push(...batch.chunkGuids);
      }
      // Live progress: unique vouchers successfully fetched + persisted so far
      progressUpdate(countedGuids.size);
    };

    const { bytes } = await fetchVouchersStream(xmlRequest, null, {
      batchSize: SYNC_BATCH_SIZE,
      onBatch,
      timeout: VOUCHER_STREAM_TIMEOUT_MS
    });

    if (windowGuids.length) {
      try {
        await cleanupWindow(fromDate, toDate, windowGuids);
      } catch (cleanupErr) {
        structuredLog("warn", `[vouchers] WINDOW CLEANUP FAILED (non-fatal): ${fromDate}-${toDate}`, {
          error: cleanupErr.message
        });
      }
    }

    structuredLog("info", `[vouchers] WINDOW DONE: ${fromDate}-${toDate} fetched=${fetchedInWindow} valid=${windowGuids.length} skipped=${skipped} size=${fmtBytes(bytes)} mem=${fmtBytes(process.memoryUsage().rss)}${outOfRange ? ` OUT-OF-RANGE=${outOfRange} (dates ${minDate}..${maxDate})` : ""}`);

    // Contiguous checkpoint: advance ONLY while every earlier window in this
    // run succeeded. After a failure, later windows are still fetched and
    // saved, but the checkpoint stays before the failed window, so the next
    // sync retries it instead of skipping that period forever.
    if (updateCheckpoint && !runHadFailure) {
      setCompanyCheckpoint(ACTIVE_COMPANY_GUID, "vouchers", fromDate, syncStartYear, syncStartMonth, {
        contiguousV: VOUCHER_CHECKPOINT_VERSION,
        passStartedAt,
        windowDays: VOUCHER_WINDOW_DAYS
      });
    }
    return { fetched: fetchedInWindow, outOfRange, minDate, maxDate };
  }

  // Incremental resume: skip already-synced windows that are fully in the past
  // AND older than the recent re-fetch period. Windows within RECENT_WINDOW_DAYS
  // of today are always re-fetched even if checkpointed, so edits/creations in
  // recent history are detected automatically.
  // RECENT_WINDOW_DAYS: how many days back to always re-fetch. Configured from
  // Main Menu -> [2] More Options -> Sync Performance (default 30).
  const RECENT_WINDOW_DAYS = syncTuning.recentWindowDays;
  const recentCutoff = new Date();
  recentCutoff.setDate(recentCutoff.getDate() - RECENT_WINDOW_DAYS);
  const recentCutoffYmd = fmtYMD(recentCutoff);

  const today = fmtYMD(new Date());
  let processed = 0;
  let resumed = 0;
  let reFetchedRecent = 0;
  // Percentage progress: based on fully COMPLETED windows (fetched+saved,
  // resumed from checkpoint, or failed-but-continued). Never exceeds 100%
  // and only reaches 100% after the final window has finished.
  let completedWindows = 0;
  // Full-book short-circuit + wedged-Tally stop (see loop below).
  let fullBookShortCircuit = false;
  let consecutiveTimeouts = 0;
  let stoppedTallyBusy = false;
  const MAX_CONSECUTIVE_TIMEOUTS = 2;
  // Count integrity: any failed window blocks checkpoint advancement (see
  // processDateRange) and is listed at the end; it is retried next sync.
  let runHadFailure = false;
  const failedWindows = [];
  let reachedEnd = false;
  // First-request plan log (§4.3)
  structuredLog("info", `[vouchers] Vouchers plan: ${syncStartYear}-${String(syncStartMonth).padStart(2, "0")}-01 -> ${today}, ${chunks.length} windows of ${VOUCHER_WINDOW_DAYS} day(s) (fetchMode=${TALLY_DETAIL_FETCH_MODE})`);

  if (TALLY_DETAIL_FETCH_MODE === "legacy") {
    const pctSuffix = () =>
      `${Math.min(100, Math.floor((completedWindows / chunks.length) * 100))}%`;
    for (let i = 0; i < chunks.length; i++) {
      // Live pause support: stop the window loop as soon as a pause is requested
      // (checkpoints are saved per completed window, so resuming later is safe).
      if (SYNC_PAUSED) {
        structuredLog("info", `[vouchers] PAUSE: stopping window loop at ${i + 1}/${chunks.length}`);
        break;
      }
      const chunk = chunks[i];
      // Skip ONLY if: already checkpointed AND fully in the past AND older than recent cutoff
      if (lastWindow && chunk.label <= lastWindow && chunk.toDate < today && chunk.toDate < recentCutoffYmd) {
        resumed++;
        completedWindows++;
        progressUpdate(countedGuids.size, pctSuffix());
        continue;
      }
      // Track re-fetches of previously-synced recent windows
      if (lastWindow && chunk.label <= lastWindow) {
        reFetchedRecent++;
      }
      structuredLog("info", `[vouchers] FETCH: window ${chunk.label} (${i + 1}/${chunks.length}) ${chunk.fromDate}-${chunk.toDate}`);
      // Keep the live counter visibly moving during full re-syncs: old windows
      // often add no new records, so the percentage keeps the line alive even
      // when the record count stays frozen.
      progressUpdate(countedGuids.size, pctSuffix());
      try {
        const win = await processDateRange(chunk.fromDate, chunk.toDate);
        processed++;
        consecutiveTimeouts = 0;

        // Full-book short-circuit (working-agent logic): Tally ignored the date
        // range and returned the whole book, which has now been fully processed
        // and saved. Every remaining window would make Tally rebuild that same
        // book again (minutes each, Tally frozen). Checkpoint to the last window
        // and stop. Only taken after a SUCCESSFUL window that returned data.
        if (win && win.fetched > 0 && win.outOfRange * 2 > win.fetched) {
          fullBookShortCircuit = true;
          completedWindows = chunks.length;
          progressUpdate(countedGuids.size, pctSuffix());
          structuredLog("warn", `[vouchers] FULL-BOOK: window ${chunk.label} returned ${win.outOfRange}/${win.fetched} voucher(s) dated outside ${chunk.fromDate}-${chunk.toDate} (${win.minDate}..${win.maxDate}) — Tally ignored the date range (usually several companies open). Saved once; skipping the remaining ${chunks.length - i - 1} window(s) this run. Checkpoint NOT advanced.`);
          progressBreak();
          console.log(`⚠️  Tally ignored the date windows and sent the whole book (${win.fetched.toLocaleString()} entries). This happens when several companies are open in Tally — close all except "${ACTIVE_COMPANY_NAME}" for a complete, fast sync.`);
          break;
        }
      } catch (windowErr) {
        runHadFailure = true;
        failedWindows.push(`${chunk.fromDate}-${chunk.toDate}`);
        structuredLog("warn", `[vouchers] WINDOW FAILED (non-fatal, will be retried next sync): ${chunk.fromDate}-${chunk.toDate}`, {
          error: windowErr.message
        });
        progressBreak();
        if (isDeveloperMode) {
          console.log(`⚠️  Window ${chunk.label} failed: ${windowErr.message} — continuing (it will be retried next sync)...`);
        }
        // Tally is single-threaded and keeps working on a request after we give
        // up on it. If windows keep timing out, Tally is wedged: stop this run
        // instead of queuing more requests behind it. The checkpoint keeps every
        // window saved so far; the next sync cycle resumes from here.
        if (windowErr.isTallyTimeout) {
          consecutiveTimeouts++;
          if (consecutiveTimeouts >= MAX_CONSECUTIVE_TIMEOUTS) {
            stoppedTallyBusy = true;
            structuredLog("warn", `[vouchers] STOPPED: ${consecutiveTimeouts} consecutive windows timed out — Tally is not responding. Remaining windows will resume on the next sync.`);
            console.log(`⚠️  Tally stopped responding (${consecutiveTimeouts} windows timed out). Voucher sync paused for this run; it will resume from the last saved window next time.`);
            completedWindows++;
            break;
          }
        } else {
          consecutiveTimeouts = 0;
        }
      }
      completedWindows++;
      progressUpdate(countedGuids.size, pctSuffix());
      if (i < chunks.length - 1) await sleep(800);
      if (i === chunks.length - 1) reachedEnd = true;
    }

    // Full-pass bookkeeping: a pass is complete only when every window from the
    // company start to today has succeeded (possibly across several runs).
    if (reachedEnd && !runHadFailure && !SYNC_PAUSED && chunks.length) {
      setCompanyCheckpoint(ACTIVE_COMPANY_GUID, "vouchers", chunks[chunks.length - 1].label, syncStartYear, syncStartMonth, {
        contiguousV: VOUCHER_CHECKPOINT_VERSION,
        passStartedAt: null,
        windowDays: VOUCHER_WINDOW_DAYS,
        fullPassAt: new Date().toISOString()
      });
      structuredLog("info", `[vouchers] FULL PASS COMPLETE: all ${chunks.length} windows up to today are synced (pass started ${passStartedAt})`);
    }
    if (failedWindows.length) {
      structuredLog("warn", `[vouchers] INCOMPLETE: ${failedWindows.length} window(s) failed and will be retried on the next sync: ${failedWindows.slice(0, 30).join(", ")}${failedWindows.length > 30 ? " ..." : ""}`);
      progressBreak();
      console.log(`⚠️  ${failedWindows.length} voucher period(s) could not be fetched from Tally this run — they will be retried automatically on the next sync.`);
    }

    if (fullBookShortCircuit || stoppedTallyBusy) {
      structuredLog("info", `[vouchers] FUTURE: discovery skipped (${fullBookShortCircuit ? "Tally returned the whole book; skipped to avoid another full-book export" : "Tally is not responding"})`);
    }
    if (!SYNC_PAUSED && !fullBookShortCircuit && !stoppedTallyBusy) {
      const horizonDays = Math.max(1, Number(process.env.TALLY_FUTURE_HORIZON_DAYS) || 365 * 5);
      const TALLY_MAX_DATE = (() => {
        const d = new Date();
        d.setDate(d.getDate() + horizonDays);
        return fmtYMD(d);
      })();
      const DISCOVERY_TIMEOUT_MS = Number(process.env.VOUCHER_DISCOVERY_TIMEOUT_MS) || 300000;
      const futureFrom = (() => {
        const d = new Date();
        d.setDate(d.getDate() + 1);
        return fmtYMD(d);
      })();
      const futureDates = new Set();
      const discovered = new Map();
      const discoveryStatus = [];

      const discoverFrom = async (label, reportName, retries = 2) => {
        const req = xmlTemplate
          .replace("<REPORTNAME>Voucher Register</REPORTNAME>", `<REPORTNAME>${reportName}</REPORTNAME>`)
          .replace("__FROM_DATE__", futureFrom)
          .replace("__TO_DATE__", TALLY_MAX_DATE);
        let found = 0;
        await fetchVouchersStream(req, null, {
          batchSize: SYNC_BATCH_SIZE,
          timeout: DISCOVERY_TIMEOUT_MS,
          retries,
          onBatch: async (items) => {
            for (const v of items) {
              const guid = extractValue(v.GUID?.[0]);
              const iso = toISODate(extractValue(v.DATE?.[0] || v.REFERENCEDATE?.[0]));
              const dYmd = iso && /^\d{4}-\d{2}-\d{2}$/.test(iso) ? iso.replace(/-/g, "") : null;
              if (!guid || !dYmd || dYmd < futureFrom) continue;
              futureDates.add(dYmd);
              if (!discovered.has(guid)) {
                discovered.set(guid, v);
                found++;
              }
            }
          }
        });
        discoveryStatus.push(`${label}=${found}`);
      };

      try {
        await discoverFrom("register", "Voucher Register");
      } catch (err) {
        discoveryStatus.push("register=FAILED");
        structuredLog("warn", `[vouchers] FUTURE: Voucher Register discovery failed (${futureFrom}-${TALLY_MAX_DATE}); will retry next sync`, {
          error: err.message
        });
        if (isDeveloperMode) console.log(`⚠️  Future-voucher discovery failed: ${err.message} — will retry next sync`);
      }

      const postDatedNames = process.env.POSTDATED_REPORT_NAME
        ? [process.env.POSTDATED_REPORT_NAME]
        : ["Post Dated Vouchers", "Post-Dated Vouchers"];
      let postDatedOk = false;
      for (const name of postDatedNames) {
        if (TALLY_MISSING_REPORTS.has(name)) {
          structuredLog("info", `[vouchers] FUTURE: skipping post-dated register "${name}" (already rejected by Tally)`);
          continue;
        }
        try {
          await discoverFrom(`postdated("${name}")`, name, 1);
          postDatedOk = true;
          break;
        } catch (err) {
          if (/LINEERROR/i.test(err.message || "")) TALLY_MISSING_REPORTS.add(name);
          structuredLog("info", `[vouchers] FUTURE: post-dated register "${name}" not available`, { error: err.message });
        }
      }
      if (!postDatedOk) discoveryStatus.push("postdated=unavailable");

      try {
        const col = saveVouchers ? "voucher_date" : "invoice_date";
        const tbl = saveVouchers ? "vouchers" : "invoices";
        const { rows } = await pool.query(
          `SELECT DISTINCT to_char(${col}, 'YYYYMMDD') AS d FROM ${tbl}
            WHERE company_guid = $1 AND ${col} >= $2::date`,
          [ACTIVE_COMPANY_GUID, futureFrom]
        );
        for (const r of rows) if (r.d) futureDates.add(r.d);
        discoveryStatus.push(`stored=${rows.length}`);
      } catch (err) {
        structuredLog("warn", "[vouchers] FUTURE: could not read stored future voucher dates", { error: err.message });
      }

      const sortedDates = [...futureDates].sort();
      const futureWindows = [];
      for (const d of sortedDates) {
        const last = futureWindows[futureWindows.length - 1];
        if (last && d <= last.limit) {
          last.toDate = d;
        } else {
          const lim = new Date(Number(d.slice(0, 4)), Number(d.slice(4, 6)) - 1, Number(d.slice(6, 8)));
          lim.setDate(lim.getDate() + VOUCHER_WINDOW_DAYS - 1);
          futureWindows.push({ fromDate: d, toDate: d, limit: fmtYMD(lim) });
        }
      }
      structuredLog("info", `[vouchers] FUTURE: discovery ${discoveryStatus.join(", ")} -> ${sortedDates.length} future date(s) in ${futureWindows.length} window(s)${sortedDates.length ? ` (${sortedDates[0]} .. ${sortedDates[sortedDates.length - 1]})` : ""}`);

      for (let i = 0; i < futureWindows.length; i++) {
        if (SYNC_PAUSED) break;
        const w = futureWindows[i];
        structuredLog("info", `[vouchers] FETCH: future window ${w.fromDate}-${w.toDate} (${i + 1}/${futureWindows.length})`);
        try {
          await processDateRange(w.fromDate, w.toDate, { updateCheckpoint: false });
          processed++;
        } catch (windowErr) {
          structuredLog("warn", `[vouchers] WINDOW FAILED (non-fatal): ${w.fromDate}-${w.toDate}`, {
            error: windowErr.message
          });
        }
        progressUpdate(countedGuids.size);
        if (i < futureWindows.length - 1) await sleep(800);
      }

      const leftovers = [...discovered.entries()].filter(([g]) => !countedGuids.has(g)).map(([, v]) => v);
      if (leftovers.length) {
        try {
          for (let i = 0; i < leftovers.length; i += 500) {
            const batch = { voucherRows: [], invoiceRows: [], chunkGuids: [] };
            for (const v of leftovers.slice(i, i + 500)) {
              totalFetched++;
              processVoucher(v, batch);
            }
            if (batch.chunkGuids.length) await writeChunk(batch);
          }
          structuredLog("info", `[vouchers] FUTURE: saved ${leftovers.length} voucher(s) directly from discovery data`);
        } catch (err) {
          structuredLog("warn", "[vouchers] FUTURE: saving discovery-only vouchers failed (non-fatal)", { error: err.message });
        }
        progressUpdate(countedGuids.size);
      }
    }
  } else {
    // ============================================================
    //  TWO-PHASE VOUCHER FETCH (§4.5) & ADAPTIVE RUNNER (§4.2)
    // ============================================================
    const controller = createWindowController({ configuredDays: VOUCHER_WINDOW_DAYS, maxDays: 7 });

    const syncCtx = {
      company: ACTIVE_COMPANY_NAME,
      companyGuid: ACTIVE_COMPANY_GUID,
      controller,
      stats: { headers: 0, changed: 0 },
      log: (msg) => structuredLog("info", `[vouchers] ${msg}`),
      onHeaders: (headers, changed) => {
        const changedGuids = new Set(changed.map((c) => c.guid));
        for (const h of headers) {
          if (!changedGuids.has(h.guid)) {
            if (!countedGuids.has(h.guid)) {
              countedGuids.add(h.guid);
              const cTypeKey = (h.type || "").trim() || "Unknown";
              if (h.cancelled) {
                cancelledCount++;
                cancelledTypeCounts[cTypeKey] = (cancelledTypeCounts[cTypeKey] || 0) + 1;
                if (!h.optional) confirmedTypeCounts[cTypeKey] = (confirmedTypeCounts[cTypeKey] || 0) + 1;
                else draftCancelled++;
              } else {
                if (h.optional) {
                  draftVouchers++;
                } else {
                  voucherTypeCounts[cTypeKey] = (voucherTypeCounts[cTypeKey] || 0) + 1;
                  confirmedTypeCounts[cTypeKey] = (confirmedTypeCounts[cTypeKey] || 0) + 1;
                }
              }
              const tLower = (h.type || "").toLowerCase();
              const isOrderType = tLower.includes("order");
              const isPurchase = !isOrderType && tLower.includes("purchase");
              const isSales = !isOrderType && (tLower.includes("sales") || tLower.includes("invoice"));
              if (saveInvoices && !h.cancelled && (isPurchase || isSales)) {
                invoiceCandidates++;
              }
            }
          }
        }
      }
    };

    const dbOps = {
      getVoucherAlterIds,
      markVouchersDeleted
    };

    const work = makeWork(syncCtx, dbOps, saveVouchersFromStream);
    const isolate = makeIsolate(syncCtx, dbOps, saveVouchersFromStream, quarantineManager);

    // Full-rescan quarantine retry (§4.7): retry quarantined items once during full re-scan
    if (!lastWindow && quarantineManager.count(ACTIVE_COMPANY_NAME) > 0) {
      structuredLog("info", `[vouchers] QUARANTINE RETRY: retrying ${quarantineManager.count(ACTIVE_COMPANY_NAME)} quarantined voucher(s) during full re-scan`);
      const qList = quarantineManager.get().filter(
        (q) => !q.company || q.company.toLowerCase() === ACTIVE_COMPANY_NAME.toLowerCase()
      );
      for (const q of qList) {
        if (q.masterId && q.day) {
          try {
            await tallyRequest(voucherOneXml(ACTIVE_COMPANY_NAME, q.day, q.masterId), {
              kind: "quarantine-retry",
              meta: { day: q.day, masterId: q.masterId },
              consumer: (res) => saveVouchersFromStream(syncCtx, res)
            });
            quarantineManager.remove(
              (item) => item.masterId === q.masterId && item.day === q.day
            );
            structuredLog("info", `[vouchers] QUARANTINE RESOLVED: masterId=${q.masterId} day=${q.day}`);
          } catch (err) {
            structuredLog("info", `[vouchers] QUARANTINE RETRY FAILED: masterId=${q.masterId} day=${q.day} still failing: ${err.message}`);
          }
        }
      }
    }

    const startDate = new Date(syncStartYear, syncStartMonth - 1, 1);
    const endDate = new Date();
    const totalDays = Math.max(1, Math.round((endDate.getTime() - startDate.getTime()) / 86400000) + 1);
    let curDate = new Date(startDate);

    const adaptivePctSuffix = () => {
      const done = Math.max(
        0,
        Math.min(totalDays, Math.round((curDate.getTime() - startDate.getTime()) / 86400000))
      );
      return `${Math.min(100, Math.floor((done / totalDays) * 100))}%`;
    };

    while (curDate <= endDate) {
      if (SYNC_PAUSED) {
        structuredLog("info", `[vouchers] PAUSE: stopping window loop at ${fmtYMD(curDate)}`);
        break;
      }
      const winDays = controller.size();
      const from = new Date(curDate);
      let to = addDays(curDate, winDays - 1);
      if (to > endDate) to = new Date(endDate);
      const fStr = fmtYMD(from);
      const tStr = fmtYMD(to);

      // Skip already-checkpointed windows fully in the past & older than recent cutoff
      if (lastWindow && fStr <= lastWindow && tStr < today && tStr < recentCutoffYmd) {
        resumed++;
        completedWindows++;
        curDate = addDays(to, 1);
        progressUpdate(countedGuids.size, adaptivePctSuffix());
        continue;
      }
      if (lastWindow && fStr <= lastWindow) {
        reFetchedRecent++;
      }

      structuredLog("info", `[vouchers] FETCH: window ${fStr}-${tStr} (${winDays}d)`);
      progressUpdate(countedGuids.size, adaptivePctSuffix());

      try {
        const res = await runAdaptive(syncCtx, from, to, work, isolate);
        processed++;
        consecutiveTimeouts = 0;
        if (res && res.quarantined && res.quarantined.length) {
          runHadFailure = true;
          failedWindows.push(...res.quarantined.map((d) => (d instanceof Date ? fmtYMD(d) : String(d))));
        } else if (!runHadFailure) {
          setCompanyCheckpoint(ACTIVE_COMPANY_GUID, "vouchers", fStr, syncStartYear, syncStartMonth, {
            contiguousV: VOUCHER_CHECKPOINT_VERSION,
            passStartedAt,
            windowDays: VOUCHER_WINDOW_DAYS
          });
        }
      } catch (windowErr) {
        runHadFailure = true;
        failedWindows.push(`${fStr}-${tStr}`);
        structuredLog("warn", `[vouchers] WINDOW FAILED (non-fatal, will be retried next sync): ${fStr}-${tStr}`, {
          error: windowErr.message
        });
        progressBreak();
        if (isDeveloperMode) {
          console.log(`⚠️  Window ${fStr}-${tStr} failed: ${windowErr.message} — continuing (it will be retried next sync)...`);
        }
        if (windowErr instanceof TallyDownError || windowErr.isTallyTimeout) {
          consecutiveTimeouts++;
          if (consecutiveTimeouts >= MAX_CONSECUTIVE_TIMEOUTS) {
            stoppedTallyBusy = true;
            structuredLog("warn", `[vouchers] STOPPED: ${consecutiveTimeouts} consecutive windows timed out — Tally is not responding. Remaining windows will resume on the next sync.`);
            console.log(`⚠️  Tally stopped responding (${consecutiveTimeouts} windows timed out). Voucher sync paused for this run; it will resume from the last saved window next time.`);
            break;
          }
        } else {
          consecutiveTimeouts = 0;
        }
      }

      completedWindows++;
      progressUpdate(countedGuids.size, adaptivePctSuffix());
      curDate = addDays(to, 1);
      if (curDate <= endDate) await sleep(500);
      if (curDate > endDate) reachedEnd = true;
    }

    if (reachedEnd && !runHadFailure && !SYNC_PAUSED) {
      setCompanyCheckpoint(ACTIVE_COMPANY_GUID, "vouchers", fmtYMD(endDate), syncStartYear, syncStartMonth, {
        contiguousV: VOUCHER_CHECKPOINT_VERSION,
        passStartedAt: null,
        windowDays: VOUCHER_WINDOW_DAYS,
        fullPassAt: new Date().toISOString()
      });
      structuredLog("info", `[vouchers] FULL PASS COMPLETE: all windows up to today are synced (pass started ${passStartedAt})`);
    }

    if (failedWindows.length) {
      structuredLog("warn", `[vouchers] INCOMPLETE: ${failedWindows.length} window(s) failed and will be retried on the next sync: ${failedWindows.slice(0, 30).join(", ")}${failedWindows.length > 30 ? " ..." : ""}`);
      progressBreak();
      console.log(`⚠️  ${failedWindows.length} voucher period(s) could not be fetched from Tally this run — they will be retried automatically on the next sync.`);
    }

    // FUTURE / POST-DATED VOUCHERS (§4.4)
    if (!SYNC_PAUSED && !stoppedTallyBusy) {
      const horizonDays = Math.max(1, Number(process.env.TALLY_FUTURE_HORIZON_DAYS) || 365 * 5);
      const futureFrom = addDays(new Date(), 1);
      const futureTo = addDays(new Date(), horizonDays);
      const futureDates = new Set();

      let chunkCur = new Date(futureFrom);
      while (chunkCur <= futureTo && !SYNC_PAUSED) {
        let chunkEnd = addDays(chunkCur, 89);
        if (chunkEnd > futureTo) chunkEnd = new Date(futureTo);
        const fStr = fmtYMD(chunkCur);
        const tStr = fmtYMD(chunkEnd);
        try {
          const xml = await tallyRequest(voucherHeadersXml(ACTIVE_COMPANY_NAME, fStr, tStr), {
            kind: "future-headers",
            meta: { from: fStr, to: tStr },
            idleMs: 60000
          });
          const hdrs = parseHeaders(xml);
          for (const h of hdrs) {
            if (h.date && h.date >= fStr) futureDates.add(h.date);
          }
        } catch (err) {
          structuredLog("warn", `[vouchers] FUTURE: 90d chunk headers failed (${fStr}-${tStr}): ${err.message}`);
        }
        chunkCur = addDays(chunkEnd, 1);
      }

      try {
        const col = saveVouchers ? "voucher_date" : "invoice_date";
        const tbl = saveVouchers ? "vouchers" : "invoices";
        const { rows } = await pool.query(
          `SELECT DISTINCT to_char(${col}, 'YYYYMMDD') AS d FROM ${tbl}
            WHERE company_guid = $1 AND ${col} >= $2::date`,
          [ACTIVE_COMPANY_GUID, fmtYMD(futureFrom)]
        );
        for (const r of rows) if (r.d) futureDates.add(r.d);
      } catch (err) {
        structuredLog("warn", "[vouchers] FUTURE: could not read stored future voucher dates", { error: err.message });
      }

      const sortedDates = [...futureDates].sort();
      const futureWindows = [];
      for (const d of sortedDates) {
        const last = futureWindows[futureWindows.length - 1];
        if (last && d <= last.limit) {
          last.toDate = d;
        } else {
          const lim = new Date(Number(d.slice(0, 4)), Number(d.slice(4, 6)) - 1, Number(d.slice(6, 8)));
          lim.setDate(lim.getDate() + VOUCHER_WINDOW_DAYS - 1);
          futureWindows.push({ fromDate: d, toDate: d, limit: fmtYMD(lim) });
        }
      }
      structuredLog("info", `[vouchers] FUTURE: ${sortedDates.length} future date(s) in ${futureWindows.length} window(s)`);

      for (let i = 0; i < futureWindows.length; i++) {
        if (SYNC_PAUSED) break;
        const w = futureWindows[i];
        const fromD = new Date(Number(w.fromDate.slice(0, 4)), Number(w.fromDate.slice(4, 6)) - 1, Number(w.fromDate.slice(6, 8)));
        const toD = new Date(Number(w.toDate.slice(0, 4)), Number(w.toDate.slice(4, 6)) - 1, Number(w.toDate.slice(6, 8)));
        try {
          await runAdaptive(syncCtx, fromD, toD, work, isolate);
          processed++;
        } catch (err) {
          structuredLog("warn", `[vouchers] FUTURE WINDOW FAILED: ${w.fromDate}-${w.toDate}`, { error: err.message });
        }
        progressUpdate(countedGuids.size);
        if (i < futureWindows.length - 1) await sleep(800);
      }
    }
  }

  progressEnd(
    countedGuids.size,
    saveVouchers && saveInvoices
      ? "Vouchers & Invoices"
      : saveVouchers
      ? "Vouchers"
      : "Invoices"
  );

  structuredLog("info", `[vouchers] VALIDATE: ${totalFetched} raw fetched, ${countedGuids.size} unique, ${skipped} skipped, ${processed} windows processed, ${resumed} windows resumed from checkpoint, ${reFetchedRecent} recent windows re-fetched`);

  // --- DB count for sync_state ---
  let dbVoucherCount = 0;
  try {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM vouchers WHERE company_guid = $1`,
      [ACTIVE_COMPANY_GUID]
    );
    dbVoucherCount = rows[0]?.n ?? 0;
  } catch {}

  try {
    const { rows } = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM vouchers WHERE company_guid = $1) AS vouchers,
         (SELECT count(*)::int FROM invoices WHERE company_guid = $1) AS invoices`,
      [ACTIVE_COMPANY_GUID]
    );
    const t = rows[0];
    devLog(
      `✅ Vouchers: ${stats.voucherSaved} new (${t.vouchers} in DB) | Invoices: ${stats.invoiceSaved} new (${t.invoices} in DB)`
    );
  } catch (e) {
    devLog(`✅ Vouchers: ${stats.voucherSaved} | Invoices: ${stats.invoiceSaved}`);
  }

  // Tally-style breakdown: Active / Cancelled / Total per voucher type
  const allTypes = new Set([
    ...Object.keys(voucherTypeCounts),
    ...Object.keys(cancelledTypeCounts)
  ]);

  // Store breakdown for the final summary (even if empty from checkpoint-resumed runs)
  const typeArr = [...allTypes].map((t) => ({
    type: t,
    active: voucherTypeCounts[t] || 0,
    cancelled: cancelledTypeCounts[t] || 0,
    total: confirmedTypeCounts[t] || 0
  }));
  typeArr.sort((a, b) => b.total - a.total);

  if (allTypes.size > 0 && isDeveloperMode) {
    // Only non-cancelled drafts are excluded from the breakdown Total;
    // cancelled drafts are counted in the Cancelled column (matching Tally).
    printVoucherTypeBreakdown(typeArr, draftNote(draftVouchers, draftList.filter((d) => !d.cancelled)));
    console.log("");
  }

  // Store for final printSyncSummary
  if (typeArr.length > 0) {
    SYNC_SUMMARY.voucherTypeBreakdown = typeArr;
    if (CURRENT_COMPANY_STATS) CURRENT_COMPANY_STATS.voucherTypeBreakdown = typeArr;
  }
  if (draftVouchers + draftCancelled > 0) {
    // Only non-cancelled drafts are excluded from the breakdown
    const draftInfo = { active: draftVouchers, cancelled: draftCancelled, list: draftList };
    SYNC_SUMMARY.voucherDrafts = draftInfo;
    if (CURRENT_COMPANY_STATS) CURRENT_COMPANY_STATS.voucherDrafts = draftInfo;
  }
  if (saveVouchers) await logSync("vouchers", stats.voucherSaved);
  if (saveInvoices) await logSync("invoices", stats.invoiceSaved);

  if (saveVouchers) {
    recordEntityStats("vouchers", {
      fetched: countedGuids.size,
      saved: stats.voucherSaved,
      failed: stats.voucherFailed,
      skipped,
      newCount: stats.voucherInserted
    });
  }
  if (saveInvoices) {
    recordEntityStats("invoices", {
      fetched: invoiceCandidates,
      saved: stats.invoiceSaved,
      failed: stats.invoiceFailed,
      skipped: 0,
      newCount: stats.invoiceInserted
    });
  }

  structuredLog("info", `[vouchers] COMPLETE: fetched=${totalFetched} unique=${countedGuids.size} invoiceCandidates=${invoiceCandidates} saved=${stats.voucherSaved} new=${stats.voucherInserted} failed=${stats.voucherFailed} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });

  // Persist incremental sync state for next run
  try {
    const syncMeta = {
      windowsProcessed: processed,
      windowsResumed: resumed,
      windowsReFetchedRecent: reFetchedRecent,
      cancelledCount,
      recentWindowDays: RECENT_WINDOW_DAYS
    };
    // Store the full voucher type breakdown so it survives incremental runs
    // where no vouchers are fetched (all windows checkpointed). This lets the
    // summary always show the last-known breakdown with cancelled counts.
    if (typeArr.length > 0) {
      syncMeta.voucherTypeBreakdown = typeArr;
    }
    await upsertSyncState(ACTIVE_COMPANY_GUID, "vouchers", {
      lastSyncAt: new Date().toISOString(),
      lastWindow: chunks.length ? chunks[chunks.length - 1].fromDate : null,
      recordCount: dbVoucherCount,
      meta: syncMeta
    });
  } catch {}
}

// ============================================================
//  4. SYNC BILLS (Receivable + Payable) — windowed & streamed
// ============================================================
async function syncBills(company) {
  return syncBillsForType("RECEIVABLE", company);
}

async function syncBillsPayable(company) {
  return syncBillsForType("PAYABLE", company);
}

// Shared bills sync. Receivable/Payable differ only in the report name and the
// bill_type tag, so one function drives both. Live-verified against TallyPrime:
// the "Bills Receivable"/"Bills Payable" report returns ALL outstanding bills
// as of SVTODATE, and a single bounded request (company start -> today) returns
// the complete current snapshot in seconds on the large company. Sending the
// full-history dates (SVFROMDATE=19000101/SVTODATE=20991231) hangs Tally, so we
// only ever send real, in-range dates. Responses are streamed through a SAX
// parser (no DOM tree), rows are upserted, stale (settled) bills are removed
// with a full snapshot replace, and a checkpoint is saved so a failed fetch is
// retried next run.
async function syncBillsForType(billType, company) {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();

  const reportName = billType === "PAYABLE" ? "Bills Payable" : "Bills Receivable";
  const entity = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";

  progressStart(`Fetching ${reportName}`);

  const { startYear: syncStartYear, startMonth: syncStartMonth } = resolveSyncPeriod(company);
  const mm = String(syncStartMonth).padStart(2, "0");
  let billsFrom = `${syncStartYear}-${mm}-01`;
  if (company && company.startingFrom && /^\d{4}-\d{2}-\d{2}$/.test(company.startingFrom)) {
    if (company.startingFrom > billsFrom) billsFrom = company.startingFrom;
  }
  const billsToday = fmtYMD(new Date());
  const windows = TALLY_DETAIL_FETCH_MODE === "legacy"
    ? [{
        fromDate: billsFrom,
        toDate: billsToday < billsFrom ? billsFrom : billsToday,
        label: billsToday
      }]
    : getBillsWindows(syncStartYear, syncStartMonth);

  // First-request plan log (§4.3)
  structuredLog("info", `[${entity}] Bills plan: ${billsFrom} -> ${billsToday}, ${windows.length} window(s) (fetchMode=${TALLY_DETAIL_FETCH_MODE})`);

  const state = getBillSyncState(billType);
  const lastWindow =
    state.startYear === syncStartYear && state.startMonth === syncStartMonth
      ? state.lastWindow || null
      : null;

  const xmlTemplate = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>${reportName}</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVCOMPANY>${ACTIVE_COMPANY_NAME}</SVCOMPANY>
     <SVFROMDATE>__FROM_DATE__</SVFROMDATE>
     <SVTODATE>__TO_DATE__</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const today = fmtYMD(new Date());
  let totalFetched = 0;
  let totalSaved = 0;
  let totalNew = 0;
  let totalFailed = 0;
  let skipped = 0;
  let processed = 0;
  let resumed = 0;
  const allSourceHashes = [];
  let hadAnyLineError = false;

  if (TALLY_DETAIL_FETCH_MODE === "legacy") {
    for (let i = 0; i < windows.length; i++) {
      const window = windows[i];
      if (lastWindow && window.label <= lastWindow && window.toDate < today) {
        resumed++;
        continue;
      }

      structuredLog("info", `[${entity}] FETCH: window ${window.label} (${i + 1}/${windows.length}) ${window.fromDate}-${window.toDate}`);

      const xmlRequest = xmlTemplate
        .replace("__FROM_DATE__", window.fromDate.replace(/-/g, ""))
        .replace("__TO_DATE__", window.toDate.replace(/-/g, ""));

      const { records, bytes, lineError } = await fetchBillsStream(xmlRequest, { entity });

      const rows = [];
      let windowSkipped = 0;
      for (const rec of records) {
        totalFetched++;
        const bill = rec.fixed;

        const ledgerName = normalizeText(bill.BILLPARTY);
        const billName = normalizeText(bill.BILLREF);
        const billDate = normalizeText(bill.BILLDATE);

        if (!ledgerName || !billName) {
          skipped++;
          windowSkipped++;
          continue;
        }

        const pendingAmt = Math.abs(parseTallyNumber(rec.cl || "0"));
        const dueDate = normalizeText(rec.due || null);
        const overdueDays = billType === "PAYABLE" ? 0 : parseTallyNumber(rec.over || "0");

        rows.push({
          company_guid: ACTIVE_COMPANY_GUID,
          ledger_name: ledgerName,
          bill_name: billName,
          bill_date: toISODate(billDate),
          bill_amount: pendingAmt,
          pending_amount: pendingAmt,
          due_date: toISODate(dueDate),
          overdue_days: overdueDays,
          bill_type: billType,
          source_hash: makeHash(
            ACTIVE_COMPANY_GUID,
            billName,
            billType,
            ledgerName,
            pendingAmt,
            dueDate
          ),
          payload: bill
        });
      }

      if (rows.length) {
        const result = await writeRowsSafe({
          table: "bills",
          columns: [
            "company_guid",
            "ledger_name",
            "bill_name",
            "bill_date",
            "bill_amount",
            "pending_amount",
            "due_date",
            "overdue_days",
            "bill_type",
            "source_hash",
            "payload"
          ],
          rows,
          conflict: ["company_guid", "source_hash"],
          updateColumns: [
            "ledger_name",
            "bill_date",
            "bill_amount",
            "pending_amount",
            "due_date",
            "overdue_days",
            "bill_type",
            "payload"
          ],
          entity,
          idFn: (r) => `${r.ledger_name}/${r.bill_name} (${r.bill_type})`
        });
        totalSaved += result.synced;
        totalNew += result.inserted || 0;
        totalFailed += result.failed.length;
      }

      if (records.length && !lineError) {
        const delRes = await pool.query(
          `DELETE FROM bills
            WHERE company_guid = $1
              AND bill_type = $2
              AND NOT (source_hash = ANY($3::text[]))
            RETURNING source_hash`,
          [
            ACTIVE_COMPANY_GUID,
            billType,
            rows.map((r) => r.source_hash)
          ]
        );
        if (delRes.rows.length) {
          structuredLog("info", `[${entity}] CLEANUP: removed ${delRes.rows.length} stale bills no longer reported`);
        }
      }

      saveBillSyncState(billType, window.label, syncStartYear, syncStartMonth);

      structuredLog("info", `[${entity}] WINDOW DONE: ${window.fromDate}-${window.toDate} fetched=${records.length} valid=${rows.length} skipped=${windowSkipped} size=${fmtBytes(bytes)}`);
      progressUpdate(totalFetched);
      processed++;

      if (i < windows.length - 1) await sleep(800);
    }
  } else {
    // Two-phase / adaptive bills sync (§4.6)
    const controller = createWindowController({ configuredDays: 365, maxDays: 366 });
    const syncCtx = {
      company: ACTIVE_COMPANY_NAME,
      companyGuid: ACTIVE_COMPANY_GUID,
      controller,
      log: (msg) => structuredLog("info", `[${entity}] ${msg}`)
    };
    const isolate = async () => ({ ok: false });

    for (let i = 0; i < windows.length; i++) {
      const window = windows[i];
      if (lastWindow && window.label <= lastWindow && window.toDate < today) {
        resumed++;
        continue;
      }

      structuredLog("info", `[${entity}] FETCH: window ${window.label} (${i + 1}/${windows.length}) ${window.fromDate}-${window.toDate}`);

      const work = async (from, to) => {
        const fStr = typeof from === "string" ? from.replace(/-/g, "") : fmtYMD(from);
        const tStr = typeof to === "string" ? to.replace(/-/g, "") : fmtYMD(to);
        const xmlRequest = xmlTemplate
          .replace("__FROM_DATE__", fStr)
          .replace("__TO_DATE__", tStr);

        const { records, bytes, lineError } = await fetchBillsStream(xmlRequest, { entity });
        if (lineError) hadAnyLineError = true;

        const rows = [];
        let windowSkipped = 0;
        for (const rec of records) {
          totalFetched++;
          const bill = rec.fixed;

          const ledgerName = normalizeText(bill.BILLPARTY);
          const billName = normalizeText(bill.BILLREF);
          const billDate = normalizeText(bill.BILLDATE);

          if (!ledgerName || !billName) {
            skipped++;
            windowSkipped++;
            continue;
          }

          const pendingAmt = Math.abs(parseTallyNumber(rec.cl || "0"));
          const dueDate = normalizeText(rec.due || null);
          const overdueDays = billType === "PAYABLE" ? 0 : parseTallyNumber(rec.over || "0");

          rows.push({
            company_guid: ACTIVE_COMPANY_GUID,
            ledger_name: ledgerName,
            bill_name: billName,
            bill_date: toISODate(billDate),
            bill_amount: pendingAmt,
            pending_amount: pendingAmt,
            due_date: toISODate(dueDate),
            overdue_days: overdueDays,
            bill_type: billType,
            source_hash: makeHash(
              ACTIVE_COMPANY_GUID,
              billName,
              billType,
              ledgerName,
              pendingAmt,
              dueDate
            ),
            payload: bill
          });
        }

        if (rows.length) {
          const result = await writeRowsSafe({
            table: "bills",
            columns: [
              "company_guid",
              "ledger_name",
              "bill_name",
              "bill_date",
              "bill_amount",
              "pending_amount",
              "due_date",
              "overdue_days",
              "bill_type",
              "source_hash",
              "payload"
            ],
            rows,
            conflict: ["company_guid", "source_hash"],
            updateColumns: [
              "ledger_name",
              "bill_date",
              "bill_amount",
              "pending_amount",
              "due_date",
              "overdue_days",
              "bill_type",
              "payload"
            ],
            entity,
            idFn: (r) => `${r.ledger_name}/${r.bill_name} (${r.bill_type})`
          });
          totalSaved += result.synced;
          totalNew += result.inserted || 0;
          totalFailed += result.failed.length;
          for (const r of rows) allSourceHashes.push(r.source_hash);
        }

        structuredLog("info", `[${entity}] WINDOW DONE: ${fStr}-${tStr} fetched=${records.length} valid=${rows.length} skipped=${windowSkipped} size=${fmtBytes(bytes)}`);
      };

      try {
        const fromD = new Date(Number(window.fromDate.slice(0, 4)), Number(window.fromDate.slice(4, 6)) - 1, Number(window.fromDate.slice(6, 8)));
        const toD = new Date(Number(window.toDate.slice(0, 4)), Number(window.toDate.slice(4, 6)) - 1, Number(window.toDate.slice(6, 8)));
        await runAdaptive(syncCtx, fromD, toD, work, isolate);
        saveBillSyncState(billType, window.label, syncStartYear, syncStartMonth);
        processed++;
      } catch (err) {
        structuredLog("warn", `[${entity}] Window ${window.label} failed: ${err.message}`);
      }

      progressUpdate(totalFetched);
      if (i < windows.length - 1) await sleep(800);
    }

    if (allSourceHashes.length && !hadAnyLineError) {
      const delRes = await pool.query(
        `DELETE FROM bills
          WHERE company_guid = $1
            AND bill_type = $2
            AND NOT (source_hash = ANY($3::text[]))
          RETURNING source_hash`,
        [
          ACTIVE_COMPANY_GUID,
          billType,
          allSourceHashes
        ]
      );
      if (delRes.rows.length) {
        structuredLog("info", `[${entity}] CLEANUP: removed ${delRes.rows.length} stale bills no longer reported`);
      }
    }
  }

  await logSync(entity, totalSaved);
  progressEnd(totalFetched, reportName);

  recordEntityStats(entity, {
    fetched: totalFetched,
    saved: totalSaved,
    failed: totalFailed,
    skipped,
    newCount: totalNew
  });

  structuredLog("info", `[${entity}] VALIDATE: ${totalFetched} fetched, ${skipped} skipped, ${processed} windows processed, ${resumed} windows resumed from checkpoint`);

  structuredLog("info", `[${entity}] COMPLETE: fetched=${totalFetched} saved=${totalSaved} new=${totalNew} failed=${totalFailed} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });
  const billDbCount = await getDbCount("bills", ACTIVE_COMPANY_GUID, "bill_type = $2", [billType]);
  devLog(`✅ ${reportName}: ${totalSaved} new (${billDbCount} in DB)`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, entity, {
      lastSyncAt: new Date().toISOString(),
      recordCount: billDbCount,
      meta: { windowsProcessed: processed, windowsResumed: resumed }
    });
  } catch {}
}

// ============================================================
//  6. SYNC ORDERS
// ============================================================
async function syncOrders() {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();
  progressStart("Fetching Orders");

  const xml = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>OrderCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
   </STATICVARIABLES>

   <TDL>
    <TDLMESSAGE>

     <COLLECTION NAME="OrderCollection">
      <TYPE>Voucher</TYPE>
      <FILTER>SalesPurchaseOrderFilter</FILTER>

      <FETCH>
        GUID,
        DATE,
        VOUCHERNUMBER,
        VOUCHERTYPENAME,
        PARTYLEDGERNAME,
        AMOUNT
      </FETCH>
     </COLLECTION>

     <SYSTEM TYPE="Formulae" NAME="SalesPurchaseOrderFilter">
       $VoucherTypeName = "Sales Order" OR
       $VoucherTypeName = "Purchase Order"
     </SYSTEM>

    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xml, { entity: "orders" });

  if (DEBUG) console.log("📥 RAW ORDER XML:\n", res.data);

  const parsed = await parseStringPromise(res.data);

  const vouchers =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.VOUCHER || [];

  const voucherArray = Array.isArray(vouchers) ? vouchers : [vouchers];

  devLog("📦 Orders Found:", voucherArray.length);
  progressUpdate(voucherArray.length);

  const rows = [];
  const orderGuids = [];
  let skipped = 0;

  for (const v of voucherArray) {
    const voucherType = v.VOUCHERTYPENAME?.[0];
    const orderGuid = v.GUID?.[0];
    if (!orderGuid) {
      skipped++;
      continue;
    }

    orderGuids.push(orderGuid);

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      order_guid: orderGuid,
      order_no: v.VOUCHERNUMBER?.[0],
      order_date: toISODate(normalizeText(v.DATE?.[0])),
      party_name: normalizeText(v.PARTYLEDGERNAME?.[0]),
      total_amount: parseTallyNumber(v.AMOUNT?.[0] || "0"),
      type: voucherType === "Sales Order" ? "Sales" : "Purchase",
      payload: v
    });
  }

  structuredLog("info", `[orders] VALIDATE: ${voucherArray.length} fetched, ${skipped} skipped, ${rows.length} valid`);

  const result = await writeRowsSafe({
    table: "orders",
    columns: [
      "company_guid",
      "order_guid",
      "order_no",
      "order_date",
      "party_name",
      "total_amount",
      "type",
      "payload"
    ],
    rows,
    conflict: ["company_guid", "order_guid"],
    updateColumns: [
      "order_no",
      "order_date",
      "party_name",
      "total_amount",
      "type",
      "payload"
    ],
    entity: "orders",
    idFn: (r) => r.order_guid
  });

  await cleanupByGuids("orders", ACTIVE_COMPANY_GUID, "order_guid", orderGuids);
  await logSync("orders", result.synced);
  progressEnd(rows.length, "Orders");

  const orderChecksum = computeGuidChecksum(orderGuids);
  const prevOrderState = await getSyncState(ACTIVE_COMPANY_GUID, "orders");
  const orderChanged = prevOrderState ? prevOrderState.checksum !== orderChecksum : true;

  recordEntityStats("orders", {
    fetched: voucherArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[orders] COMPLETE: fetched=${voucherArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${orderChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const orderDbCount = await getDbCount("orders", ACTIVE_COMPANY_GUID);
  devLog(`✅ Orders: ${result.synced} new (${orderDbCount} in DB)${orderChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "orders", {
      lastSyncAt: new Date().toISOString(),
      recordCount: voucherArray.length,
      checksum: orderChecksum,
      meta: { changed: orderChanged }
    });
  } catch {}
}

// ============================================================
//  7. INVENTORY — FULL STOCK
// ============================================================
async function fetchFullStockData() {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();
  progressStart("Fetching Inventory Items");

  const xml = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>StockItemCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="StockItemCollection">
      <TYPE>StockItem</TYPE>
      <FETCH>
        NAME,
        GUID,
        PARENT,
        BASEUNITS,
        OPENINGBALANCE,
        OPENINGVALUE,
        CLOSINGBALANCE,
        CLOSINGVALUE
      </FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xml, { entity: "inventory_items" });

  const parsed = await parseStringPromise(res.data);

  const items =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.STOCKITEM || [];

  const itemArray = Array.isArray(items) ? items : [items];

  devLog("📦 Stock items from Tally:", itemArray.length);
  progressUpdate(itemArray.length);

  const rows = [];
  const itemGuids = [];
  let skipped = 0;

  for (const i of itemArray) {
    const itemName = normalizeText(i.NAME?.[0]) || i.$?.NAME || null;
    if (!itemName) {
      skipped++;
      continue;
    }

    const itemGuid = normalizeText(i.GUID?.[0]) || i.$?.GUID || null;
    if (!itemGuid) {
      skipped++;
      continue;
    }

    itemGuids.push(itemGuid);

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      item_guid: itemGuid,
      name: itemName,
      item_group: normalizeText(i.PARENT?.[0]),
      unit: normalizeText(i.BASEUNITS?.[0]),
      opening_qty: parseTallyNumber(i.OPENINGBALANCE?.[0]),
      opening_value: parseTallyNumber(i.OPENINGVALUE?.[0]),
      closing_qty: parseTallyNumber(i.CLOSINGBALANCE?.[0]),
      closing_value: parseTallyNumber(i.CLOSINGVALUE?.[0]),
      payload: i
    });
  }

  structuredLog("info", `[inventory_items] VALIDATE: ${itemArray.length} fetched, ${skipped} skipped, ${rows.length} valid`);

  const result = await writeRowsSafe({
    table: "inventory_items",
    columns: [
      "company_guid",
      "item_guid",
      "name",
      "item_group",
      "unit",
      "opening_qty",
      "opening_value",
      "closing_qty",
      "closing_value",
      "payload"
    ],
    rows,
    conflict: ["company_guid", "item_guid"],
    updateColumns: [
      "name",
      "item_group",
      "unit",
      "opening_qty",
      "opening_value",
      "closing_qty",
      "closing_value",
      "payload"
    ],
    entity: "inventory_items",
    idFn: (r) => r.item_guid
  });

  await cleanupByGuids("inventory_items", ACTIVE_COMPANY_GUID, "item_guid", itemGuids);
  await logSync("inventory_items", result.synced);
  progressEnd(rows.length, "Inventory Items");

  const invChecksum = computeGuidChecksum(itemGuids);
  const prevInvState = await getSyncState(ACTIVE_COMPANY_GUID, "inventory_items");
  const invChanged = prevInvState ? prevInvState.checksum !== invChecksum : true;

  recordEntityStats("inventory_items", {
    fetched: itemArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[inventory_items] COMPLETE: fetched=${itemArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${invChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const invDbCount = await getDbCount("inventory_items", ACTIVE_COMPANY_GUID);
  devLog(`✅ Full inventory: ${result.synced} new (${invDbCount} in DB)${invChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "inventory_items", {
      lastSyncAt: new Date().toISOString(),
      recordCount: itemArray.length,
      checksum: invChecksum,
      meta: { changed: invChanged }
    });
  } catch {}
}

// ============================================================
//  MAIN SYNC ORCHESTRATOR
// ============================================================
async function syncAllData(companies, dataTypes = "all") {
  if (isSyncRunning) {
    console.log("⏳ Previous sync still running. Skipping...");
    return;
  }

  isSyncRunning = true;
  const syncStart = Date.now();
  SYNC_RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");
  SYNC_SUMMARY.startedAt = new Date();
  SYNC_SUMMARY.endedAt = null;
  SYNC_SUMMARY.companiesSynced = 0;
  SYNC_SUMMARY.companyGuids = [];
  SYNC_SUMMARY.entities = {};
  SYNC_SUMMARY.voucherTypeBreakdown = null;
  SYNC_SUMMARY.companies = [];
  CURRENT_COMPANY_STATS = null;

  const wants = (key) => dataTypes === "all" || dataTypes.includes(key);
  const selectedLabel = dataTypes === "all"
    ? "ALL"
    : dataTypes.join(", ");

  structuredLog("info", "Sync started", { companies: companies.length, dataTypes: selectedLabel });
  console.log("🔄 Sync started...");

  try {
    recordEntityStats("companies", {
      fetched: companies.length,
      saved: companies.length,
      failed: 0,
      skipped: 0
    });

    let pausedMidRun = false;
    companyLoop:
    for (const company of companies) {
      if (SYNC_PAUSED) { pausedMidRun = true; break; }
      const companyStart = Date.now();
      console.log("\n🔐 Syncing company:", company.name);
      structuredLog("info", `[company] Sync started: ${company.name}`, {
        companyGuid: company.company_guid
      });

      ACTIVE_COMPANY_GUID = company.company_guid;
      ACTIVE_COMPANY_NAME = company.name;
      SYNC_SUMMARY.companyGuids.push(company.company_guid);

      // Company-open check (§4.9): ensure Tally is up and the selected company is open
      await waitForTally(company.name, { reason: "company not open" });

      // Per-company summary bucket — each company gets its own completely
      // separate Fetched/New/Failed/Skipped/In DB counts and voucher breakdown.
      CURRENT_COMPANY_STATS = {
        guid: company.company_guid,
        name: company.name,
        entities: {},
        voucherTypeBreakdown: null
      };
      SYNC_SUMMARY.companies.push(CURRENT_COMPANY_STATS);

      // Entity steps for this company. Vouchers and invoices share a single
      // "Voucher Register" Tally request; run it once whenever either is
      // selected and save only what was chosen.
      const steps = [
        ["ledgers", "➡️ syncLedgers", () => syncLedgers(company)],
        ["bills_receivable", "➡️ syncBills (Receivable)", () => syncBills(company)],
        ["bills_payable", "➡️ syncBills (Payable)", () => syncBillsPayable(company)],
        ["vouchers", "➡️ syncVouchers", () => syncVouchers(company, {
          saveVouchers: wants("vouchers"),
          saveInvoices: wants("invoices")
        })],
        ["orders", "➡️ syncOrders", () => syncOrders()],
        ["inventory_items", "➡️ fetchFullStockData", () => fetchFullStockData()]
      ];

      for (const [key, label, run] of steps) {
        if (SYNC_PAUSED) { pausedMidRun = true; break companyLoop; }
        const shouldRun = key === "vouchers" ? (wants("vouchers") || wants("invoices")) : wants(key);
        if (!shouldRun) continue;

        // Heavy entity cooldown (§4.6): cooldown + health check before Bills and Vouchers
        if (key === "bills_receivable" || key === "bills_payable" || key === "vouchers") {
          devLog(`[tally] Heavy entity cooldown (${TALLY_HEAVY_COOLDOWN_MS}ms) before ${key}...`);
          progressStatus(`cooldown ${Math.round(TALLY_HEAVY_COOLDOWN_MS / 1000)}s`);
          await sleep(TALLY_HEAVY_COOLDOWN_MS);
          progressStatus(null);
          await ensureTallyHealthy(company.name);
        }

        devLog(label);
        await run();
      }

      if (SYNC_PAUSED) { pausedMidRun = true; break; }

      structuredLog("info", `[company] Sync completed: ${company.name}`, {
        duration: formatDuration(Date.now() - companyStart)
      });
      console.log(`✅ Sync completed for ${company.name}`);
      SYNC_SUMMARY.companiesSynced++;
    }

    CURRENT_COMPANY_STATS = null;

    if (pausedMidRun) {
      structuredLog("info", "[sync] Paused by user mid-run");
      console.log("\n⏸  Sync paused by user.");
    } else {
      SYNC_SUMMARY.endedAt = new Date();
      await printSyncSummary(syncStart);

      console.log("\n✅ Sync complete!");
    }
  } catch (err) {
    SYNC_SUMMARY.endedAt = new Date();
    await printSyncSummary(syncStart);
    structuredLog("error", "Sync FAILED", { error: err.message, stack: err.stack });
    console.error("⛔ SYNC FAILED");
    console.error("MESSAGE:", err.message);
  } finally {
    isSyncRunning = false;
  }
}

// ============================================================
//  INTERACTIVE COMPANY SELECTION
// ============================================================
async function selectCompanies(allCompanies) {
  console.log("\n🏢 Companies available for sync:");
  allCompanies.forEach((c, i) => {
    console.log(`   [${i + 1}] ${c.name}`);
  });
  console.log("   [0] ALL companies");

  const answer = await askQuestion(
    "\nSelect company number(s) (comma separated, e.g. 1,3), '0' for all, or B to go back: "
  );

  const trimmed = answer.trim().toLowerCase();

  if (trimmed === "b") {
    return null;
  }

  if (trimmed === "0" || trimmed === "all") {
    console.log(`✅ Selected ALL companies (${allCompanies.length})`);
    return allCompanies;
  }

  const selected = [];
  for (const part of trimmed.split(",")) {
    const idx = parseInt(part.trim(), 10);
    if (!isNaN(idx) && idx >= 1 && idx <= allCompanies.length) {
      selected.push(allCompanies[idx - 1]);
    }
  }

  if (!selected.length) {
    console.log("⚠️ No valid selection. Defaulting to ALL companies.");
    return allCompanies;
  }

  console.log(
    "✅ Selected:",
    selected.map((c) => c.name).join(", ")
  );
  return selected;
}

// ============================================================
//  INTERACTIVE DATA-TYPE SELECTION
// ============================================================
const DATA_TYPES = [
  { key: "ledgers", label: "Ledgers" },
  { key: "bills_receivable", label: "Bills Receivable" },
  { key: "bills_payable", label: "Bills Payable" },
  { key: "vouchers", label: "Vouchers" },
  { key: "invoices", label: "Invoices" },
  { key: "orders", label: "Orders" },
  { key: "inventory_items", label: "Inventory Items" }
];

async function selectDataTypes() {
  console.log("\n📊 Data to sync:");
  DATA_TYPES.forEach((d, i) => {
    console.log(`   [${i + 1}] ${d.label}`);
  });
  console.log("   [0] ALL data");

  const answer = await askQuestion(
    "\nSelect data type number(s) (comma separated, e.g. 2,4), '0' for ALL data, or B to go back: "
  );

  const trimmed = answer.trim().toLowerCase();

  if (trimmed === "b") {
    return null;
  }

  if (trimmed === "0" || trimmed === "all") {
    console.log("✅ Selected ALL data types");
    return "all";
  }

  const selected = [];
  for (const part of trimmed.split(",")) {
    const idx = parseInt(part.trim(), 10);
    if (!isNaN(idx) && idx >= 1 && idx <= DATA_TYPES.length) {
      selected.push(DATA_TYPES[idx - 1].key);
    }
  }

  if (!selected.length) {
    console.log("⚠️ No valid selection. Defaulting to ALL data types.");
    return "all";
  }

  console.log(
    "✅ Selected:",
    selected.map((k) => DATA_TYPES.find((d) => d.key === k).label).join(", ")
  );
  return selected;
}

// ============================================================
//  SELECTIVE SYNC CONFIGURATION (interactive)
//  Lets the user enable/disable selective sync and pick a
//  starting year + month.  The setting persists to disk so it
//  survives restarts — editable from the main menu as well.
// ============================================================
async function configureSelectiveSync() {
  console.log("\n📅 Selective Sync — Pick Starting Period");
  console.log("   When enabled, sync fetches data from a specific year & month");
  console.log("   instead of the full company history (or auto-detected FY start).\n");

  const current = selectiveSyncConfig;
  if (current.enabled) {
    console.log(`   Current setting: ENABLED  →  ${MONTH_NAMES[current.startMonth - 1]} ${current.startYear}`);
  } else {
    console.log("   Current setting: DISABLED (auto / full history)");
  }

  const enableAns = (await askQuestion("\n   Enable selective sync? (Y/N, B to go back): ")).trim().toLowerCase();
  if (enableAns === "b") return null;
  if (enableAns !== "y" && enableAns !== "yes") {
    selectiveSyncConfig = { enabled: false, startYear: null, startMonth: 4 };
    saveSyncConfig(selectiveSyncConfig);
    console.log("   ✅ Selective sync DISABLED — using auto-detected or full history.\n");
    return selectiveSyncConfig;
  }

  // ── Pick starting month ────────────────────────────────
  console.log("\n   Select starting month:");
  for (let i = 0; i < 12; i++) {
    const marker = (current.startMonth === i + 1) ? " (current)" : "";
    console.log(`     [${String(i + 1).padStart(2, "0")}] ${MONTH_NAMES[i]}${marker}`);
  }

  const monthAns = (await askQuestion(`\n   Month number [01-12, default ${String(current.startMonth).padStart(2, "0")}]: `)).trim();
  let startMonth = parseInt(monthAns, 10);
  if (isNaN(startMonth) || startMonth < 1 || startMonth > 12) {
    startMonth = current.startMonth || 4;
    console.log(`   Using default month: ${MONTH_NAMES[startMonth - 1]}`);
  } else {
    console.log(`   ✅ Month: ${MONTH_NAMES[startMonth - 1]}`);
  }

  // ── Pick starting year ─────────────────────────────────
  const now = new Date();
  const currentYear = now.getFullYear();
  const defaultYear = current.startYear || currentYear;

  const yearAns = (await askQuestion(`\n   Starting year (e.g. 2020, default ${defaultYear}): `)).trim();
  let startYear = parseInt(yearAns, 10);
  if (isNaN(startYear) || startYear < 1900 || startYear > currentYear + 1) {
    startYear = defaultYear;
    console.log(`   Using default year: ${startYear}`);
  } else {
    console.log(`   ✅ Year: ${startYear}`);
  }

  selectiveSyncConfig = { enabled: true, startYear, startMonth };
  saveSyncConfig(selectiveSyncConfig);
  console.log(`\n   ✅ Selective sync ENABLED → ${MONTH_NAMES[startMonth - 1]} ${startYear} to today\n`);
  return selectiveSyncConfig;
}

// Resolve the actual calendar Date from which to start syncing.
// Priority: env var SYNC_START_YEAR → selective sync config → company startingFrom → 1900-01-01
// Returns { startYear, startMonth } (1-indexed month).
function resolveSyncPeriod(company) {
  // 1. Environment variable overrides everything
  const envYear = Number(process.env.SYNC_START_YEAR);
  if (envYear && envYear > 1900 && envYear < 3000) {
    return { startYear: envYear, startMonth: 4 }; // env var is year-only, default April
  }

  // 2. Selective sync config (interactive setting)
  if (selectiveSyncConfig && selectiveSyncConfig.enabled && selectiveSyncConfig.startYear) {
    return { startYear: selectiveSyncConfig.startYear, startMonth: selectiveSyncConfig.startMonth || 4 };
  }

  // 3. Company auto-detected startingFrom date
  if (company && company.startingFrom && /^\d{4}-\d{2}-\d{2}$/.test(company.startingFrom)) {
    return {
      startYear: Number(company.startingFrom.slice(0, 4)),
      startMonth: Number(company.startingFrom.slice(5, 7)) || 4
    };
  }

  // 4. Default: full history from Jan 1900
  return { startYear: 1900, startMonth: 1 };
}

// ============================================================
//  SYNC FLOW (company selection + data type selection + timer + sync loop)
// ============================================================
async function runSyncFlow() {
  try {
    SYNC_RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");

    // Step 1: Detect companies + auto-create in DB
    const companies = await detectCompanies();

    if (!companies.length) {
      console.error("❌ No companies found in Tally.");
      return;
    }

    // Steps 2-4: pre-sync configuration screens. Typing B steps back one
    // screen; backing out of company selection returns to the main menu.
    let selectedCompanies = null;
    let selectedDataTypes = null;
    let minutesAnswer = "";

    while (true) {
      // Step 2: Let the user pick one company or ALL
      selectedCompanies = await selectCompanies(companies);
      if (!selectedCompanies) {
        console.log("↩️ Back to main menu.\n");
        return;
      }

      // Step 3: Let the user pick which data types to sync (one or many)
      selectedDataTypes = await selectDataTypes();
      if (!selectedDataTypes) {
        console.log("\n↩️ Back to company selection.\n");
        continue;
      }

      // Step 4: Auto-sync interval (minutes). 0 = keep syncing continuously
      // (defaults to a 5 minute gap between runs).
      minutesAnswer = await askQuestion(
        "\nEnter sync interval in minutes (0 = continuous, every 5 min, B = back): "
      );
      if (minutesAnswer.trim().toLowerCase() === "b") {
        console.log("\n↩️ Back to data selection.\n");
        continue;
      }
      break;
    }

    let minutes = parseFloat(minutesAnswer) || 0;
    let loopWaitMs = minutes > 0 ? minutes * 60 * 1000 : 5 * 60 * 1000;
    const loopMinutes = Math.round(loopWaitMs / 60000);
    SYNC_INTERVAL = loopWaitMs;

    if (minutes > 0) {
      console.log(`⏱ Sync will repeat every ${minutes} minute(s). Press P anytime to pause & reconfigure.\n`);
    } else {
      console.log("⏱ Running continuous sync (every 5 min). Press P anytime to pause & reconfigure, Ctrl+C to stop.\n");
    }

    // Live control: pressing P at any time pauses after the current step and
    // opens the reconfiguration menu (change companies / data / interval).
    SYNC_PAUSED = false;
    const stopPauseListener = installPauseListener();

    try {
      await syncAllData(selectedCompanies, selectedDataTypes);

      // Step 5: Auto-sync loop. It NEVER stops on its own — if a sync run
      // fails, we log it and keep going until the user pauses or closes the
      // terminal (Ctrl+C).
      console.log(`\n🔁 Auto-sync loop started (every ${loopMinutes} min). Press P to pause & reconfigure, Ctrl+C to stop.`);
      // When the user resumes from the pause menu they expect the new
      // configuration to sync IMMEDIATELY — not after waiting a full interval.
      let syncNow = false;
      while (true) {
        if (SYNC_PAUSED) {
          SYNC_PAUSED = false;
          const next = await showPauseMenu({ companies: selectedCompanies, dataTypes: selectedDataTypes, minutes });
          if (!next || next.action === "stop") {
            console.log("\n⏹ Syncing stopped — back to main menu.\n");
            return;
          }
          selectedCompanies = next.companies;
          selectedDataTypes = next.dataTypes;
          minutes = next.minutes;
          loopWaitMs = minutes > 0 ? minutes * 60 * 1000 : 5 * 60 * 1000;
          SYNC_INTERVAL = loopWaitMs;
          syncNow = true;
          console.log(`\n🔁 Resuming auto-sync (every ${Math.max(1, Math.round(loopWaitMs / 60000))} min) for ${describeSelection(selectedCompanies, selectedDataTypes)}\n`);
        }

        try {
          if (!syncNow) {
            // Interruptible wait: poll the pause flag every second so pressing
            // P during the gap takes effect immediately instead of after the
            // full interval.
            const waitStart = Date.now();
            while (!SYNC_PAUSED && Date.now() - waitStart < loopWaitMs) {
              const remain = loopWaitMs - (Date.now() - waitStart);
              await new Promise((r) => setTimeout(r, Math.max(0, Math.min(1000, remain))));
            }
            if (SYNC_PAUSED) continue; // open menu immediately without syncing
            structuredLog("info", `[auto-sync] interval ${Math.round(loopWaitMs / 60000)} min elapsed, starting next sync`);
          } else {
            syncNow = false;
            console.log("▶️ Syncing now with the updated configuration...");
          }
          await syncAllData(selectedCompanies, selectedDataTypes);
        } catch (err) {
          structuredLog("error", "[auto-sync] Loop iteration failed; continuing", {
            error: err.message,
            stack: err.stack
          });
        }
      }
    } finally {
      stopPauseListener();
    }
  } catch (err) {
    console.error("⛔ FATAL ERROR:", err.message);
  }
}

// ============================================================
//  PAUSE & LIVE RECONFIGURATION
//  While a sync/auto-sync cycle is active, pressing P requests a pause.
//  The current step finishes, then this menu lets the user change the
//  company selection (add/remove), data types or interval and resume,
//  or stop syncing entirely and return to the main menu.
// ============================================================
function describeSelection(companies, dataTypes) {
  const companyText = Array.isArray(companies)
    ? companies.length === 1
      ? companies[0].name
      : `${companies.length} companies`
    : "companies";
  return `${companyText} [${dataTypes === "all" ? "ALL data" : dataTypes.join(", ")}]`;
}

function installPauseListener() {
  const stdin = process.stdin;
  let rawEnabled = false;

  const onData = (buf) => {
    const s = buf.toString();
    // In raw mode Ctrl+C no longer kills the process by default — keep that behavior.
    if (s === "\u0003") process.exit(0);
    if (PAUSE_LISTENER_SUSPENDED) return;
    if (/^[\s\r\n]*p[\s\r\n]*$/i.test(s)) {
      SYNC_PAUSED = true;
      console.log("\n\n⏸  Pause requested — finishing the current step...");
    }
  };

  try {
    if (stdin.isTTY && typeof stdin.setRawMode === "function") {
      stdin.setRawMode(true);
      rawEnabled = true;
    }
  } catch {}
  try { stdin.resume(); } catch {}
  stdin.on("data", onData);

  return function stopPauseListener() {
    stdin.removeListener("data", onData);
    try {
      if (rawEnabled && typeof stdin.setRawMode === "function") stdin.setRawMode(false);
    } catch {}
    SYNC_PAUSED = false;
  };
}

async function showPauseMenu(current) {
  PAUSE_LISTENER_SUSPENDED = true;
  try {
    let companies = current.companies;
    let dataTypes = current.dataTypes;
    let minutes = current.minutes;

    while (true) {
      const companyText = `${companies.length} selected — ${companies.map((c) => c.name).join(", ")}`;
      const dataText = dataTypes === "all"
        ? "ALL"
        : DATA_TYPES.filter((d) => dataTypes.includes(d.key)).map((d) => d.label).join(", ");

      console.log("\n========================================");
      console.log("⏸  SYNC PAUSED — CURRENT CONFIGURATION");
      console.log("========================================");
      console.log(`   Companies : ${companyText}`);
      console.log(`   Data types: ${dataText}`);
      console.log(`   Interval  : every ${minutes > 0 ? minutes : 5} min`);
      const selPeriod = selectiveSyncConfig && selectiveSyncConfig.enabled
        ? `${MONTH_NAMES[selectiveSyncConfig.startMonth - 1]} ${selectiveSyncConfig.startYear} → today`
        : "Auto / full history";
      console.log(`   Start from: ${selPeriod}`);
      console.log(`   Windows   : ${syncTuning.windowDays} day(s) per Tally request`);
      console.log(`   Batches   : ${syncTuning.batchSize} record(s) per database write`);
      console.log("----------------------------------------");
      console.log("  [1] Change companies (add / remove)");
      console.log("  [2] Change data types");
      console.log("  [3] Change interval");
      console.log("  [4] Resume syncing now");
      console.log("  [5] Stop & return to main menu");

      const choice = (await askQuestion("\nSelect an option: ")).trim();

      if (choice === "1") {
        // Re-detect so companies opened/closed in Tally Prime show up.
        let fresh = companies;
        try { fresh = await detectCompanies(); } catch {}
        const sel = await selectCompanies(fresh);
        if (sel && sel.length) companies = sel; // B / empty keeps the current selection
      } else if (choice === "2") {
        const sel = await selectDataTypes();
        if (sel) dataTypes = sel;
      } else if (choice === "3") {
        const ans = await askQuestion("Enter sync interval in minutes (0 = continuous/every 5 min): ");
        const val = parseFloat(ans);
        if (!isNaN(val) && val >= 0) {
          minutes = val;
          console.log(`✅ Interval set to every ${val > 0 ? val : 5} min`);
        } else {
          console.log("⚠️ Invalid interval — keeping current setting.");
        }
      } else if (choice === "4") {
        return { action: "resume", companies, dataTypes, minutes };
      } else if (choice === "5") {
        return { action: "stop" };
      } else {
        console.log("⚠️ Invalid option. Please choose 1-5.");
      }
    }
  } finally {
    PAUSE_LISTENER_SUSPENDED = false;
  }
}

// ============================================================
//  RESET & FULL RE-SYNC
//  Clears all checkpoints and sync_state so the next sync
//  fetches EVERYTHING from Tally fresh with the latest filters
//  (cancelled voucher exclusion, etc.).
// ============================================================
async function resetAndFullSync() {
  console.log("\n⚠️  RESET & FULL RE-SYNC");
  console.log("This will:");
  console.log("  • Delete all checkpoint/resume data");
  console.log("  • Clear sync_state (incremental metadata)");
  console.log("  • Re-fetch ALL data from Tally on next sync");
  console.log("  • Apply cancelled voucher filter to ALL vouchers\n");

  const confirm = await askQuestion("Type 'YES' to confirm reset (B to cancel): ");
  if (confirm.trim().toUpperCase() !== "YES") {
    console.log("❌ Reset cancelled. Nothing was cleared.\n");
    return;
  }

  // 1. Delete checkpoint file
  try {
    if (fs.existsSync(SYNC_STATE_FILE)) {
      fs.unlinkSync(SYNC_STATE_FILE);
      console.log("  ✅ Deleted checkpoint file:", SYNC_STATE_FILE);
    } else {
      console.log("  ℹ️  No checkpoint file found (already clean)");
    }
  } catch (err) {
    console.log("  ⚠️ Could not delete checkpoint file:", err.message);
  }

  // 2. Clear sync_state table
  try {
    await pool.query("DELETE FROM sync_state");
    console.log("  ✅ Cleared sync_state table");
  } catch (err) {
    console.log("  ⚠️ Could not clear sync_state:", err.message);
  }

  // 3. Optionally clear old vouchers so re-sync is clean
  const clearDb = await askQuestion("\nAlso DELETE all existing vouchers/invoices from DB? (recommended for accurate counts) [y/N]: ");
  if (clearDb.trim().toLowerCase() === "y" || clearDb.trim().toLowerCase() === "yes") {
    try {
      console.log("  🗑️  Deleting invoices...");
      await pool.query("DELETE FROM invoices");
      console.log("  🗑️  Deleting vouchers...");
      await pool.query("DELETE FROM vouchers");
      console.log("  🗑️  Deleting sync_log...");
      await pool.query("DELETE FROM sync_log");
      console.log("  ✅ All voucher/invoice data cleared");
    } catch (err) {
      console.log("  ⚠️ Could not clear tables:", err.message);
    }
  }

  console.log("\n✅ Reset complete! Starting full re-sync...\n");

  // 4. Trigger a full sync
  await runSyncFlow();
}

// ============================================================
//  BOOTSTRAP
// ============================================================
async function bootstrap() {
  console.log("\n🟢 TallyScrapper Middleware starting...\n");

  // ── Step 1: Authentication Menu ──────────────────────────
  await startAuthenticationFlow();

  // ── Step 2: Connect to database ──────────────────────────
  let cfg = loadDbConfig();

  if (cfg) {
    console.log("📂 Saved database configuration found. Connecting...");
    try {
      await connectWithConfig(cfg);
      console.log("✅ Connected automatically to the saved database.\n");
    } catch (err) {
      console.log(`⚠️ Could not connect using the saved configuration: ${err.message}`);
      console.log("Please update your database settings.\n");
      cfg = await dbWizard();
      while (!cfg) {
        console.log("⚠️ A working database connection is required to continue.");
        cfg = await dbWizard();
      }
      await connectWithConfig(cfg);
      console.log("✅ Connected with new settings.\n");
    }
  } else {
    console.log("🛠️ First launch detected — let's set up your database.\n");
    cfg = await dbWizard();
    while (!cfg) {
      console.log("⚠️ A working database connection is required to continue.");
      cfg = await dbWizard();
    }
    await connectWithConfig(cfg);
  }

  // ── Step 3: Main menu loop ───────────────────────────────
  let running = true;

  // Database switch helper — shared by the main menu and More Options.
  const changeDatabase = async () => {
    console.log("\n🛠️ Change Database Configuration\n");
    const newCfg = await dbWizard();
    if (!newCfg) {
      console.log("↩️ Returned without changing the database.\n");
      return;
    }
    try {
      await connectWithConfig(newCfg);
      console.log("✅ Database switched successfully.\n");
    } catch (err) {
      console.error(`❌ Could not connect with the new settings: ${err.message}`);
    }
  };

  while (running) {
    const choice = await mainMenu();

    if (choice === "1") {
      await runSyncFlow();
    } else if (choice === "2") {
      // More Options — a small sub-menu, looped until the user goes back.
      while (true) {
        const more = await moreOptionsMenu();
        if (more === "1") {
          await configureSyncPerformance();
        } else if (more === "2") {
          await changeDatabase();
        } else if (more === "3") {
          await tallyPortWizard();
        } else if (more === "b" || more === "B") {
          console.log("\n↩️ Back to main menu.\n");
          break;
        } else {
          console.log("\n⚠️ Invalid option. Please choose 1, 2, 3 or B.");
        }
      }
    } else if (choice === "3") {
      await tallyPortWizard();
    } else if (choice === "4") {
      await configureSelectiveSync();
    } else if (choice === "5") {
      await resetAndFullSync();
    } else if (choice === "6") {
      // Logout deletes the saved login; Exit ([7]) never does.
      const loggedOut = await logout();
      if (loggedOut) {
        await startAuthenticationFlow();
      }
    } else if (choice === "7") {
      running = false;
    } else {
      console.log("⚠️ Invalid option. Please choose 1-7.");
    }
  }

  await pool.end();
  console.log("👋 Goodbye! Your login is saved — just start the agent again to continue.");
  process.exit(0);
}

// When required as a module (tests / automation), do NOT boot the interactive CLI.
if (require.main === module) {
  // Record every exit (code + memory) so a closed window always leaves a trace.
  process.on("exit", (code) => {
    try {
      const m = process.memoryUsage();
      writeJsonLine(logFilePath("sync"), {
        ts: new Date().toISOString(),
        level: code ? "error" : "info",
        msg: `PROCESS EXIT code=${code}`,
        rss: fmtBytes(m.rss),
        heapUsed: fmtBytes(m.heapUsed),
        syncId: SYNC_RUN_ID
      });
    } catch {}
  });
  bootstrap().catch(async (err) => {
    structuredLog("error", "Bootstrap FAILED", { error: err.message, stack: err.stack });
    console.error("⛔ BOOTSTRAP FAILED:", err.message);
    console.error(`   Details were saved in ${LOG_DIR}`);
    // Keep the window open so the reason can be read (an EXE window would
    // otherwise close immediately).
    if (process.stdin && process.stdin.isTTY) {
      try {
        if (process.stdin.setRawMode) process.stdin.setRawMode(false);
        await new Promise((r) => {
          const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
          rl.question("\nPress Enter to close...", () => { rl.close(); r(); });
        });
      } catch {}
    }
    process.exit(1);
  });
}

module.exports = {
  bootstrap,
  isDeveloperModeEnabled: () => isDeveloperMode,
  developerLogin,
  setActiveCompany: (guid, name) => {
    ACTIVE_COMPANY_GUID = guid;
    ACTIVE_COMPANY_NAME = name;
  },
  getActiveCompany: () => ({ guid: ACTIVE_COMPANY_GUID, name: ACTIVE_COMPANY_NAME }),
  loadDbConfig,
  connectWithConfig,
  getPool: () => pool,
  detectCompanies,
  saxVoucherStream,
  fetchVouchersStream,
  saxBillsStream,
  fetchBillsStream,
  syncLedgers,
  syncVouchers,
  syncOrders,
  fetchFullStockData,
  syncBills,
  syncBillsPayable,
  syncBillsForType,
  resolveSyncStartYear,
  resolveSyncPeriod,
  getVoucherWindows,
  getBillsWindows,
  loadSyncConfig,
  saveSyncConfig,
  configureSelectiveSync,
  loadSyncTuning,
  saveSyncTuning,
  getTallyThrottleState,
  isTallyUnhealthy,
  __setTallyUrl: (url) => { TALLY_URL = url; },
  __postTallyXml: postTallyXml,
  configureSyncPerformance,
  moreOptionsMenu,
  mainMenu,
  loadLoginSession,
  clearLoginSession,
  logout,
  syncAllData,
  selectDataTypes,
  selectCompanies,
  DATA_TYPES,
  getCompanyCheckpoint,
  setCompanyCheckpoint,
  getBillSyncState,
  saveBillSyncState,
  loadSyncState,
  saveSyncState,
  normalizeTallyDate,
  parseStartingFromName,
  fmtYMD
};
