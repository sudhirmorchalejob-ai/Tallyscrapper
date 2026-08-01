require("dotenv").config({ quiet: true });

const axios = require("axios");
const Agent = require("agentkeepalive");
const xml2js = require("xml2js");
const { parseStringPromise } = xml2js;
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const readline = require("readline");

const DEBUG = false;

// ============================================================
//  HTTP AGENTS (Tally + long XML responses)
// ============================================================
const httpAgent = new Agent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: 60000,
  freeSocketTimeout: 30000
});

const httpsAgent = new Agent.HttpsAgent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: 60000,
  freeSocketTimeout: 30000
});

axios.defaults.httpAgent = httpAgent;
axios.defaults.httpsAgent = httpsAgent;
axios.defaults.timeout = 600000;

// ============================================================
//  TALLY + DATABASE CONFIG
// ============================================================
// Tally Prime HTTP endpoint (ODBC / XML interface) on port 9000
const TALLY_URL = "http://localhost:9000";

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
async function dbWizard() {
  console.log("\n🛠️  Database Configuration Wizard");
  console.log("======================================");

  while (true) {
    const mode = await askQuestion(
      "\nDatabase type — [1] Local PostgreSQL, [2] External PostgreSQL: "
    );

    let cfg = { mode: mode === "2" ? "external" : "local" };

    if (mode === "2") {
      const useUrl = (await askQuestion("Use a connection URL? [y/N]: ")).trim().toLowerCase();
      if (useUrl === "y") {
        cfg.connectionString = await askQuestion(
          "Connection URL (postgres://user:pass@host:port/db): "
        );
      } else {
        cfg.host = await askQuestion("Host: ");
        cfg.port = (await askQuestion("Port [5432]: ")) || "5432";
        cfg.database = await askQuestion("Database name: ");
        cfg.user = await askQuestion("Username: ");
        cfg.password = await askQuestion("Password: ", true);
      }
    } else {
      cfg.host = (await askQuestion("Host [localhost]: ")) || "localhost";
      cfg.port = (await askQuestion("Port [5432]: ")) || "5432";
      cfg.database = await askQuestion("Database name: ");
      cfg.user = await askQuestion("Username: ");
      cfg.password = await askQuestion("Password: ", true);
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
   MAIN MENU
---------------------------- */
async function mainMenu() {
  console.log("\n================= TALLY AGENT =================");
  console.log("  [1] Start Sync");
  console.log("  [2] Database Settings (Change Database Configuration)");
  console.log("  [3] Exit");
  console.log("=================================================");

  const choice = (await askQuestion("\nSelect an option: ")).trim();
  return choice;
}

// ============================================================
//  APP STATE
// ============================================================
let ACTIVE_COMPANY_GUID = null;
let ACTIVE_COMPANY_NAME = null;
let SYNC_INTERVAL = 0; // ms, set by user
let isSyncRunning = false;
const STATE_FILE = path.join(os.homedir(), "tally-agent-state.json");

// ============================================================
//  HELPERS
// ============================================================
function askQuestion(query, hideInput = false) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  if (hideInput) {
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
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, voucher_guid)
);

CREATE TABLE IF NOT EXISTS voucher_entries (
  id           BIGSERIAL PRIMARY KEY,
  company_guid TEXT NOT NULL,
  voucher_guid TEXT NOT NULL,
  ledger_name  TEXT NOT NULL,
  amount       NUMERIC DEFAULT 0,
  is_debit     BOOLEAN DEFAULT FALSE
);

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
  UNIQUE (company_guid, source_hash)
);

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

CREATE TABLE IF NOT EXISTS inventory_movements (
  id            BIGSERIAL PRIMARY KEY,
  company_guid  TEXT NOT NULL,
  item_name     TEXT,
  qty           NUMERIC DEFAULT 0,
  movement_type TEXT,
  voucher_type  TEXT,
  voucher_date  DATE,
  source_hash   TEXT NOT NULL,
  UNIQUE (company_guid, source_hash)
);

CREATE TABLE IF NOT EXISTS profit_loss (
  id           BIGSERIAL PRIMARY KEY,
  company_guid TEXT NOT NULL,
  month        TEXT,
  from_date    DATE,
  to_date      DATE,
  net_amount   NUMERIC DEFAULT 0,
  type         TEXT,
  UNIQUE (company_guid, month)
);

CREATE TABLE IF NOT EXISTS sync_log (
  id             BIGSERIAL PRIMARY KEY,
  company_guid   TEXT,
  company_name   TEXT,
  entity         TEXT,
  records        INT,
  synced_at      TIMESTAMPTZ DEFAULT NOW()
);
`;

// Add raw JSON payload column to every table (works for existing DBs too)
const PAYLOAD_COLUMNS = `
ALTER TABLE companies ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE ledgers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE vouchers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE voucher_entries ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE bills ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE profit_loss ADD COLUMN IF NOT EXISTS payload JSONB;
`;

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(SCHEMA);
    await client.query(PAYLOAD_COLUMNS);
    console.log("✅ Database tables ready");
  } finally {
    client.release();
  }
}

// Generic upsert (duplicate-free)
// - updateColumns: non-empty  => ON CONFLICT DO UPDATE
// - updateColumns: empty list => ON CONFLICT DO NOTHING (immutable events)
async function upsertRows({ table, columns, rows, conflict, updateColumns }) {
  if (!rows || !rows.length) return 0;

  const placeholders = rows
    .map((_, i) => `(${columns.map((_, j) => `$${i * columns.length + j + 1}`).join(",")})`)
    .join(",");

  const values = rows.flatMap((r) =>
    columns.map((c) => (r[c] === undefined || r[c] === "" ? null : r[c]))
  );

  const conflictTarget = Array.isArray(conflict)
    ? `(${conflict.join(", ")})`
    : conflict;

  let sql;
  if (updateColumns.length) {
    const setClause = updateColumns.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ");
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO UPDATE SET ${setClause}`;
  } else {
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO NOTHING`;
  }

  await pool.query(sql, values);
  return rows.length;
}

// Delete records for a company that no longer exist in Tally
async function cleanupByGuids(table, companyGuid, guidColumn, guids) {
  if (!guids || !guids.length) return;
  const placeholders = guids.map((_, i) => `$${i + 2}`).join(", ");
  const sql = `DELETE FROM "${table}" WHERE company_guid = $1 AND ${guidColumn} NOT IN (${placeholders})`;
  const res = await pool.query(sql, [companyGuid, ...guids]);
  console.log(`🧹 Cleanup ${table}: removed ${res.rowCount} missing records`);
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
async function detectCompanies() {
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

  const response = await axios.post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" }
  });

  const jsonData = await parseStringPromise(response.data);

  const companies =
    jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];

  const companyList = [];

  for (const c of companies) {
    const company_guid =
      typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;
    const name =
      typeof c.NAME?.[0] === "string" ? c.NAME[0] : c.NAME?.[0]?._;

    if (!company_guid || !name) continue;

    companyList.push({ company_guid, name, payload: c });
  }

  // 🔥 Auto-create companies in DB (duplicate-free upsert)
  await upsertRows({
    table: "companies",
    columns: ["company_guid", "name", "payload"],
    rows: companyList,
    conflict: ["company_guid"],
    updateColumns: ["name", "payload"]
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

  const res = await axios.post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" }
  });

  const parser = new xml2js.Parser({ explicitArray: false });
  const parsed = await parser.parseStringPromise(res.data);

  const ledgers = parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.LEDGER || [];
  const ledgerArray = Array.isArray(ledgers) ? ledgers : [ledgers];

  console.log("Ledgers from Tally:", ledgerArray.length);

  const ledgerRows = [];
  const ledgerGuids = [];

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
      console.log("⚠️ Skipping ledger without name");
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
      console.log("⏭️ Skipping invalid ledger:", ledgerName);
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

  const synced = await upsertRows({
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
    ]
  });

  await cleanupByGuids("ledgers", company.company_guid, "ledger_guid", ledgerGuids);
  await logSync("ledgers", synced);
  console.log(`✅ Ledgers synced: ${synced}`);
}

// ============================================================
//  3. SYNC VOUCHERS (+ INVOICES + ENTRIES)
// ============================================================
async function syncVouchers() {
  if (!ACTIVE_COMPANY_GUID) return;

  const voucherRows = [];
  const invoiceRows = [];
  const entryRows = [];
  const voucherGuids = [];

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>VoucherCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="VoucherCollection">
      <TYPE>Voucher</TYPE>
      <FETCH>
        GUID,
        DATE,
        VOUCHERTYPENAME,
        VOUCHERNUMBER,
        REFERENCE,
        PARTYLEDGERNAME,
        ALLLEDGERENTRIES.LIST,
        LEDGERENTRIES.LIST
      </FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await axios.post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" }
  });

  const parsed = await parseStringPromise(res.data);

  const vouchers =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.VOUCHER || [];

  const msgArray = Array.isArray(vouchers) ? vouchers : [vouchers];

  function extractValue(val) {
    if (!val) return null;
    if (typeof val === "string") return val;
    if (typeof val === "object" && val._) return val._;
    return null;
  }

  for (const v of msgArray) {
    const voucherGuid = extractValue(v.GUID?.[0]);
    if (!voucherGuid) continue;

    voucherGuids.push(voucherGuid);

    const rawDate = extractValue(v.DATE?.[0] || v.REFERENCEDATE?.[0]);
    const voucherDate = toISODate(rawDate);
    const voucherType = extractValue(v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]);
    const referenceNo =
      extractValue(v.REFERENCE?.[0] || v.REFERENCENUMBER?.[0] || v.VOUCHERNUMBER?.[0]);

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

    voucherRows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      voucher_guid: voucherGuid,
      voucher_date: voucherDate,
      voucher_type: voucherType,
      reference_no: referenceNo,
      net_amount: netAmount,
      payload: v
    });

    entryRows.push(...entries);

    const isPurchase =
      voucherType && voucherType.toLowerCase().includes("purchase");
    const isSales =
      voucherType &&
      (voucherType.toLowerCase().includes("sales") ||
        voucherType.toLowerCase().includes("invoice"));

    if (isPurchase || isSales) {
      invoiceRows.push({
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

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const voucherCount = await upsertRows({
      table: "vouchers",
      columns: [
        "company_guid",
        "voucher_guid",
        "voucher_date",
        "voucher_type",
        "reference_no",
        "net_amount",
        "payload"
      ],
      rows: voucherRows,
      conflict: ["company_guid", "voucher_guid"],
      updateColumns: [
        "voucher_date",
        "voucher_type",
        "reference_no",
        "net_amount",
        "payload"
      ]
    });

    const invoiceCount = await upsertRows({
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
      rows: invoiceRows,
      conflict: ["company_guid", "invoice_guid"],
      updateColumns: [
        "invoice_no",
        "invoice_date",
        "invoice_type",
        "party_name",
        "total_amount",
        "payload"
      ]
    });

    // 🔥 Rebuild entries for the synced vouchers (no duplicates)
    if (voucherGuids.length) {
      await client.query(
        "DELETE FROM voucher_entries WHERE company_guid = $1 AND voucher_guid = ANY($2::text[])",
        [ACTIVE_COMPANY_GUID, voucherGuids]
      );
    }
    let entryCount = 0;
    if (entryRows.length) {
      const placeholders = entryRows
        .map(
          (_, i) =>
            `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`
        )
        .join(",");
      const values = entryRows.flatMap((r) => [
        r.company_guid,
        r.voucher_guid,
        r.ledger_name,
        r.amount,
        r.is_debit,
        r.payload
      ]);
      await client.query(
        `INSERT INTO voucher_entries (company_guid, voucher_guid, ledger_name, amount, is_debit, payload)
         VALUES ${placeholders}`,
        values
      );
      entryCount = entryRows.length;
    }

    await cleanupByGuids("vouchers", ACTIVE_COMPANY_GUID, "voucher_guid", voucherGuids);
    await client.query("COMMIT");

    console.log(`✅ Vouchers: ${voucherCount} | Invoices: ${invoiceCount} | Entries: ${entryCount}`);
    await logSync("vouchers", voucherCount);
    await logSync("invoices", invoiceCount);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// ============================================================
//  4. SYNC BILLS (Receivable)
// ============================================================
async function syncBills() {
  if (!ACTIVE_COMPANY_GUID) return;

  console.log("🧾 Syncing Bills Receivable...");

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Bills Receivable</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVCOMPANY>${ACTIVE_COMPANY_NAME}</SVCOMPANY>
     <SVFROMDATE>19000101</SVFROMDATE>
<SVTODATE>20991231</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const res = await axios.post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" }
  });

  const parsed = await parseStringPromise(res.data);
  const envelope = parsed?.ENVELOPE || {};

  const billFixedArr = Array.isArray(envelope.BILLFIXED)
    ? envelope.BILLFIXED
    : envelope.BILLFIXED
    ? [envelope.BILLFIXED]
    : [];
  const billAmounts = Array.isArray(envelope.BILLCL)
    ? envelope.BILLCL
    : envelope.BILLCL
    ? [envelope.BILLCL]
    : [];
  const billDueArr = Array.isArray(envelope.BILLDUE)
    ? envelope.BILLDUE
    : envelope.BILLDUE
    ? [envelope.BILLDUE]
    : [];
  const billOverArr = Array.isArray(envelope.BILLOVERDUE)
    ? envelope.BILLOVERDUE
    : envelope.BILLOVERDUE
    ? [envelope.BILLOVERDUE]
    : [];

  const rows = [];

  for (let i = 0; i < billFixedArr.length; i++) {
    const bill = billFixedArr[i];

    const ledgerName = normalizeText(bill.BILLPARTY);
    const billName = normalizeText(bill.BILLREF);
    const billDate = normalizeText(bill.BILLDATE);

    if (!ledgerName || !billName) continue;

    const pendingAmt = Math.abs(parseTallyNumber(billAmounts[i] || "0"));
    const dueDate = normalizeText(billDueArr[i] || null);
    const overdueDays = parseTallyNumber(billOverArr[i] || "0");

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      ledger_name: ledgerName,
      bill_name: billName,
      bill_date: toISODate(billDate),
      bill_amount: pendingAmt,
      pending_amount: pendingAmt,
      due_date: toISODate(dueDate),
      overdue_days: overdueDays,
      bill_type: "RECEIVABLE",
      source_hash: makeHash(
        ACTIVE_COMPANY_GUID,
        billName,
        "RECEIVABLE",
        ledgerName,
        pendingAmt,
        dueDate
      ),
      payload: bill
    });
  }

  const synced = await upsertRows({
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
    ]
  });

  await logSync("bills_receivable", synced);
  console.log(`✅ Bills Receivable synced: ${synced}`);
}

// ============================================================
//  5. SYNC BILLS PAYABLE
// ============================================================
async function syncBillsPayable() {
  if (!ACTIVE_COMPANY_GUID) return;

  console.log("🧾 Syncing Bills Payable...");

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Bills Payable</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVFROMDATE>19000101</SVFROMDATE>
<SVTODATE>20991231</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const res = await axios.post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" }
  });

  const parsed = await parseStringPromise(res.data);
  const envelope = parsed?.ENVELOPE || {};

  const billFixedArr = Array.isArray(envelope.BILLFIXED)
    ? envelope.BILLFIXED
    : envelope.BILLFIXED
    ? [envelope.BILLFIXED]
    : [];
  const billAmounts = Array.isArray(envelope.BILLCL)
    ? envelope.BILLCL
    : envelope.BILLCL
    ? [envelope.BILLCL]
    : [];
  const billDueArr = Array.isArray(envelope.BILLDUE)
    ? envelope.BILLDUE
    : envelope.BILLDUE
    ? [envelope.BILLDUE]
    : [];

  const rows = [];

  for (let i = 0; i < billFixedArr.length; i++) {
    const bill = billFixedArr[i];

    const ledgerName = normalizeText(bill.BILLPARTY);
    const billName = normalizeText(bill.BILLREF);
    const billDate = normalizeText(bill.BILLDATE);

    if (!ledgerName || !billName) {
      console.log("⏭️ Skipping invalid payable bill:", bill);
      continue;
    }

    const pendingAmt = Math.abs(parseTallyNumber(billAmounts[i] || "0"));
    const dueDate = normalizeText(billDueArr[i] || null);

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      ledger_name: ledgerName,
      bill_name: billName,
      bill_date: toISODate(billDate),
      bill_amount: pendingAmt,
      pending_amount: pendingAmt,
      due_date: toISODate(dueDate),
      overdue_days: 0,
      bill_type: "PAYABLE",
      source_hash: makeHash(
        ACTIVE_COMPANY_GUID,
        billName,
        "PAYABLE",
        ledgerName,
        pendingAmt,
        dueDate
      ),
      payload: bill
    });
  }

  const synced = await upsertRows({
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
      "bill_type",
      "payload"
    ]
  });

  await logSync("bills_payable", synced);
  console.log(`✅ Bills Payable synced: ${synced}`);
}

// ============================================================
//  6. SYNC ORDERS
// ============================================================
async function syncOrders() {
  if (!ACTIVE_COMPANY_GUID) return;

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

  const res = await axios.post(TALLY_URL, xml, {
    headers: { "Content-Type": "text/xml" }
  });

  if (DEBUG) console.log("📥 RAW ORDER XML:\n", res.data);

  const parsed = await parseStringPromise(res.data);

  const vouchers =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.VOUCHER || [];

  const voucherArray = Array.isArray(vouchers) ? vouchers : [vouchers];

  console.log("📦 Orders Found:", voucherArray.length);

  const rows = [];
  const orderGuids = [];

  for (const v of voucherArray) {
    const voucherType = v.VOUCHERTYPENAME?.[0];
    const orderGuid = v.GUID?.[0];
    if (!orderGuid) continue;

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

  const synced = await upsertRows({
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
    ]
  });

  await cleanupByGuids("orders", ACTIVE_COMPANY_GUID, "order_guid", orderGuids);
  await logSync("orders", synced);
  console.log(`✅ Orders synced: ${synced}`);
}

// ============================================================
//  7. INVENTORY — FULL STOCK
// ============================================================
async function fetchFullStockData() {
  if (!ACTIVE_COMPANY_GUID) return;

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

  const res = await axios.post(TALLY_URL, xml, {
    headers: { "Content-Type": "text/xml" }
  });

  const parsed = await parseStringPromise(res.data);

  const items =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.STOCKITEM || [];

  const itemArray = Array.isArray(items) ? items : [items];

  console.log("📦 Stock items from Tally:", itemArray.length);

  const rows = [];
  const itemGuids = [];

  for (const i of itemArray) {
    const itemName = normalizeText(i.NAME?.[0]) || i.$?.NAME || null;
    if (!itemName) continue;

    const itemGuid = normalizeText(i.GUID?.[0]) || i.$?.GUID || null;
    if (!itemGuid) continue;

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

  const synced = await upsertRows({
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
    ]
  });

  await cleanupByGuids("inventory_items", ACTIVE_COMPANY_GUID, "item_guid", itemGuids);
  await logSync("inventory_items", synced);
  console.log(`✅ Full inventory synced: ${synced}`);
}

// ============================================================
//  8. INVENTORY MOVEMENTS (IN / OUT)
// ============================================================
async function syncInventoryMovements() {
  if (!ACTIVE_COMPANY_GUID) return;

  console.log("📦 Syncing Inventory Movements...");

  const xml = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Day Book</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVFROMDATE>20240401</SVFROMDATE>
     <SVTODATE>20251231</SVTODATE>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const res = await axios.post(TALLY_URL, xml, {
    headers: { "Content-Type": "text/xml" }
  });

  if (res.data.includes("<LINEERROR>")) {
    throw new Error("Inventory movement report error");
  }

  const parsed = await parseStringPromise(res.data);

  const messages =
    parsed?.ENVELOPE?.BODY?.[0]?.IMPORTDATA?.[0]?.REQUESTDATA?.[0]
      ?.TALLYMESSAGE || [];

  const msgArray = Array.isArray(messages) ? messages : [messages];

  console.log("📦 Inventory vouchers:", msgArray.length);

  const rows = [];
  const seen = new Set();

  for (const msg of msgArray) {
    const v = msg.VOUCHER?.[0];
    if (!v) continue;

    const voucherType = v.VOUCHERTYPENAME?.[0];
    const voucherDate = toISODate(v.DATE?.[0]);

    if (!["Sales", "Purchase"].includes(voucherType)) continue;

    const invEntries = [
      ...(Array.isArray(v["ALLINVENTORYENTRIES.LIST"])
        ? v["ALLINVENTORYENTRIES.LIST"]
        : v["ALLINVENTORYENTRIES.LIST"]
        ? [v["ALLINVENTORYENTRIES.LIST"]]
        : []),
      ...(Array.isArray(v["INVENTORYENTRIES.LIST"])
        ? v["INVENTORYENTRIES.LIST"]
        : v["INVENTORYENTRIES.LIST"]
        ? [v["INVENTORYENTRIES.LIST"]]
        : [])
    ];

    if (!invEntries.length) continue;

    const movementType =
      voucherType === "Purchase" ? "IN" :
      voucherType === "Sales" ? "OUT" : null;

    if (!movementType) continue;

    for (const e of invEntries) {
      const itemName = e.STOCKITEMNAME?.[0];
      if (!itemName) continue;

      const qty = parseTallyNumber(
        e.BILLEDQTY?.[0] || e.ACTUALQTY?.[0] || "0"
      );
      if (!qty) continue;

      const source_hash = makeHash(
        ACTIVE_COMPANY_GUID,
        itemName,
        Math.abs(qty),
        movementType,
        voucherType,
        voucherDate
      );

      if (seen.has(source_hash)) continue;
      seen.add(source_hash);

      rows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        item_name: itemName,
        qty: Math.abs(qty),
        movement_type: movementType,
        voucher_type: voucherType,
        voucher_date: voucherDate,
        source_hash,
        payload: e
      });
    }
  }

  const synced = await upsertRows({
    table: "inventory_movements",
    columns: [
      "company_guid",
      "item_name",
      "qty",
      "movement_type",
      "voucher_type",
      "voucher_date",
      "source_hash",
      "payload"
    ],
    rows,
    conflict: ["company_guid", "source_hash"],
    updateColumns: [
      "item_name",
      "qty",
      "movement_type",
      "voucher_type",
      "voucher_date",
      "payload"
    ]
  });

  await logSync("inventory_movements", synced);
  console.log(`✅ Inventory movements synced: ${synced}`);
}

// ============================================================
//  9. PROFIT & LOSS (MONTHLY)
// ============================================================
async function syncProfitLossMonthly() {
  if (!ACTIVE_COMPANY_GUID) return;

  console.log("📊 Syncing Monthly Profit & Loss...");

  const startYear = 2025;
  const now = new Date();

  for (let year = startYear; year <= now.getFullYear(); year++) {
    for (let month = 0; month < 12; month++) {
      const fromDateObj = new Date(year, month, 1);
      const toDateObj = new Date(year, month + 1, 0);
      if (fromDateObj > now) break;

      const fromDate = fromDateObj.toISOString().slice(0, 10).replace(/-/g, "");
      const toDate = toDateObj.toISOString().slice(0, 10).replace(/-/g, "");

      const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Profit & Loss A/c</REPORTNAME>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
      <SVFROMDATE>${fromDate}</SVFROMDATE>
      <SVTODATE>${toDate}</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

      try {
        const res = await axios.post(TALLY_URL, xmlRequest, {
          headers: { "Content-Type": "text/xml" }
        });

        if (res.data.includes("<LINEERROR>")) {
          console.log("⚠️ P&L report error for", fromDate);
          continue;
        }

        const parsed = await parseStringPromise(res.data);

        let netAmount = 0;
        let type = "PROFIT";

        const netProfitValue =
          parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]
            ?.NETPROFIT?.[0] ||
          parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.NETPROFIT?.[0] ||
          parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.DSPNETPROFIT?.[0];

        if (netProfitValue) {
          netAmount = parseTallyNumber(netProfitValue);
          type = netAmount >= 0 ? "PROFIT" : "LOSS";
        }

        if (netAmount === 0) continue;

        const rows = [
          {
            company_guid: ACTIVE_COMPANY_GUID,
            month: `${year}-${String(month + 1).padStart(2, "0")}`,
            from_date: toISODate(fromDate),
            to_date: toISODate(toDate),
            net_amount: Math.abs(netAmount),
            type,
            payload: {
              from_date: fromDate,
              to_date: toDate,
              net_amount: netAmount,
              type
            }
          }
        ];

        const synced = await upsertRows({
          table: "profit_loss",
          columns: [
            "company_guid",
            "month",
            "from_date",
            "to_date",
            "net_amount",
            "type",
            "payload"
          ],
          rows,
          conflict: ["company_guid", "month"],
          updateColumns: ["from_date", "to_date", "net_amount", "type", "payload"]
        });

        await logSync("profit_loss", synced);
      } catch (err) {
        console.error("❌ P&L sync error:", err.message);
      }
    }
  }

  console.log("✅ Profit & Loss Monthly Sync Complete");
}

// ============================================================
//  MAIN SYNC ORCHESTRATOR
// ============================================================
async function syncAllData(companies) {
  if (isSyncRunning) {
    console.log("⏳ Previous sync still running. Skipping...");
    return;
  }

  isSyncRunning = true;
  console.log("🔄 Sync started...");

  try {
    for (const company of companies) {
      console.log("\n🔐 Syncing company:", company.name);

      ACTIVE_COMPANY_GUID = company.company_guid;
      ACTIVE_COMPANY_NAME = company.name;

      console.log("➡️ STEP 1: syncLedgers");
      await syncLedgers(company);

      console.log("➡️ STEP 2: syncVouchers");
      await syncVouchers();

      console.log("➡️ STEP 3: syncOrders");
      await syncOrders();

      console.log("➡️ STEP 4: syncInventoryMovements");
      await syncInventoryMovements();

      console.log("➡️ STEP 5: fetchFullStockData");
      await fetchFullStockData();

      console.log("➡️ STEP 6: syncBills");
      await syncBills();
      await syncBillsPayable();

      // console.log("➡️ STEP 7: syncProfitLossMonthly");
      // await syncProfitLossMonthly();

      console.log(`✅ Sync completed for ${company.name}`);
    }

    console.log("\n✅ Sync complete!");
  } catch (err) {
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
    "\nSelect company number(s) (comma separated, e.g. 1,3) or '0' for all: "
  );

  const trimmed = answer.trim().toLowerCase();

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
//  SYNC FLOW (company selection + timer + sync loop)
// ============================================================
async function runSyncFlow() {
  try {
    // Step 1: Detect companies + auto-create in DB
    const companies = await detectCompanies();

    if (!companies.length) {
      console.error("❌ No companies found in Tally.");
      return;
    }

    // Step 2: Let the user pick one company or ALL
    const selectedCompanies = await selectCompanies(companies);

    // Step 3: Manual sync timer (minutes)
    const minutesAnswer = await askQuestion(
      "\nEnter sync interval in minutes (0 = sync once): "
    );
    const minutes = parseFloat(minutesAnswer) || 0;
    SYNC_INTERVAL = minutes * 60 * 1000;

    if (minutes > 0) {
      console.log(`⏱ Sync will repeat every ${minutes} minute(s)\n`);
    } else {
      console.log("⏱ Running single sync (no timer)\n");
    }

    await syncAllData(selectedCompanies);

    // Step 4: Loop on the manual timer
    if (SYNC_INTERVAL > 0) {
      console.log(`\n🔁 Auto-sync loop started (every ${minutes} min). Press Ctrl+C to stop.`);
      while (true) {
        await new Promise((r) => setTimeout(r, SYNC_INTERVAL));
        await syncAllData(selectedCompanies);
      }
    }
  } catch (err) {
    console.error("⛔ FATAL ERROR:", err.message);
  }
}

// ============================================================
//  BOOTSTRAP
// ============================================================
(async () => {
  console.log("\n🟢 Tally → Postgres Direct Agent starting...\n");

  // ── Step 1: Connect to database ──────────────────────────
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
      await connectWithConfig(cfg);
      console.log("✅ Connected with new settings.\n");
    }
  } else {
    console.log("🛠️ First launch detected — let's set up your database.\n");
    cfg = await dbWizard();
    await connectWithConfig(cfg);
  }

  // ── Step 2: Main menu loop ───────────────────────────────
  let running = true;

  while (running) {
    const choice = await mainMenu();

    if (choice === "1") {
      await runSyncFlow();
    } else if (choice === "2") {
      console.log("\n🛠️ Change Database Configuration\n");
      const newCfg = await dbWizard();
      try {
        await connectWithConfig(newCfg);
        console.log("✅ Database switched successfully.\n");
      } catch (err) {
        console.error(`❌ Could not connect with the new settings: ${err.message}`);
      }
    } else if (choice === "3") {
      running = false;
    } else {
      console.log("⚠️ Invalid option. Please choose 1, 2 or 3.");
    }
  }

  await pool.end();
  console.log("👋 Goodbye!");
  process.exit(0);
})();
