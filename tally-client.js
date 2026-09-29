'use strict';
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');

function getTallyPort() {
  if (process.env.TALLY_PORT) return Number(process.env.TALLY_PORT) || 9000;
  try {
    const portFile = path.join(os.homedir(), 'tally-agent-port.json');
    if (fs.existsSync(portFile)) {
      const data = JSON.parse(fs.readFileSync(portFile, 'utf8'));
      if (data && data.port) return Number(data.port) || 9000;
    }
  } catch {}
  return 9000;
}

const CFG = {
  get port() { return getTallyPort(); },
  minGapMs: +process.env.TALLY_MIN_GAP_MS || 3000,
  timeoutMs: +process.env.TALLY_TIMEOUT_MS || 180000,
  maxWaitMin: +process.env.TALLY_MAX_WAIT_MIN || 120,
  memWarnMb: +process.env.TALLY_MEM_WARN_MB || 2500,
  memMaxMb: +process.env.TALLY_MEM_MAX_MB || 3500,
};

const LOG_DIR = path.join(os.homedir(), 'tally-agent-logs');
try { fs.mkdirSync(LOG_DIR, { recursive: true }); } catch {}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

class TallyDownError extends Error {
  constructor(m, code) {
    super(m);
    this.name = 'TallyDownError';
    this.code = code;
  }
}

class TallyLogicalError extends Error {
  constructor(m) {
    super(m);
    this.name = 'TallyLogicalError';
  }
}

const DOWN_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'EPIPE', 'ETIMEDOUT', 'ECONNABORTED']);

function classify(err) {
  if (!err) return err;
  if (err instanceof TallyLogicalError || err instanceof TallyDownError) return err;
  if (DOWN_CODES.has(err.code) || /socket hang up|TALLY_TIMEOUT|aborted|ETIMEDOUT|ECONNRESET/i.test(err.message || '')) {
    return new TallyDownError(err.message || 'Tally connection error', err.code);
  }
  return err;
}

// ---------- write-ahead request log (flushed BEFORE sending) ----------
function logReq(kind, info) {
  try {
    const f = path.join(LOG_DIR, `requests-${new Date().toISOString().slice(0, 10)}.log`);
    fs.appendFileSync(f, JSON.stringify({ t: new Date().toISOString(), kind, ...info }) + '\n');
  } catch {}
}

// ---------- single-flight queue + adaptive gap ----------
const agent = new http.Agent({ keepAlive: false, maxSockets: 1 });
let chain = Promise.resolve();
const gate = { nextAt: 0, lastMs: 0 };

function withTallyThrottle(fn) {
  const run = chain.then(async () => {
    const wait = gate.nextAt - Date.now();
    if (wait > 0) await sleep(wait);
    const t0 = Date.now();
    try {
      return await fn();
    } finally {
      gate.lastMs = Date.now() - t0;
      gate.nextAt = Date.now() + Math.max(CFG.minGapMs, Math.min(gate.lastMs * 1.5, 30000));
    }
  });
  chain = run.catch(() => {});
  return run;
}

function bufferBody(res) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    res.on('data', (c) => chunks.push(c));
    res.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      if (/<LINEERROR>/i.test(body)) {
        const lineErr = body.match(/<LINEERROR>([^<]*)/i)?.[1] || 'Tally line error';
        return reject(new TallyLogicalError(lineErr));
      }
      resolve(body);
    });
    res.on('error', (e) => reject(classify(e)));
  });
}

/** consumer(res) -> Promise. Default consumer buffers to string. */
function tallyRequest(xml, { consumer, idleMs = CFG.timeoutMs, kind = 'req', meta = {} } = {}) {
  const reqBytes = Buffer.byteLength(xml);
  logReq(kind, { ...meta, bytes: reqBytes });

  return withTallyThrottle(() => new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port: CFG.port,
        method: 'POST',
        agent,
        headers: {
          'Content-Type': 'text/xml; charset=utf-8',
          'Content-Length': reqBytes
        }
      },
      (res) => {
        const done = consumer ? consumer(res) : bufferBody(res);
        Promise.resolve(done).then(resolve, (e) => reject(classify(e)));
        res.on('error', (e) => reject(classify(e)));
        res.on('aborted', () => reject(new TallyDownError('response aborted', 'ECONNRESET')));
      }
    );

    req.setTimeout(idleMs, () => {
      req.destroy(new Error('TALLY_TIMEOUT'));
    });

    req.on('error', (e) => reject(classify(e)));
    req.end(xml);
  }));
}

const sanitizeXml = (s) =>
  String(s || '')
    .replace(/&#(?:0*[0-8]|0*1[12]|0*1[4-9]|0*2[0-9]|0*3[01]);/g, '')
    .replace(/&#x0*(?:[0-8bBcC]|[eEfF]|1[0-9a-fA-F]);/g, '')
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');

// ---------- health ----------
const listCompaniesXml = () => `<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
<TYPE>Collection</TYPE><ID>CompList</ID></HEADER><BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
<TDL><TDLMESSAGE><COLLECTION NAME="CompList"><TYPE>Company</TYPE><FETCH>Name</FETCH></COLLECTION>
</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>`;

/** Tiny request. Returns {alive, companies[]} without throwing. */
async function probeTally() {
  try {
    const body = await tallyRequest(listCompaniesXml(), { idleMs: 8000, kind: 'probe' });
    const companies = [...body.matchAll(/<NAME[^>]*>([^<]+)<\/NAME>/gi)].map((m) => m[1].trim());
    return { alive: /<ENVELOPE/i.test(body), companies };
  } catch {
    return { alive: false, companies: [] };
  }
}

/** Windows only: tally*.exe running? working-set MB? Returns null on other OS. */
function tallyProcess() {
  return new Promise((resolve) => {
    if (process.platform !== 'win32') return resolve(null);
    execFile('tasklist', ['/FI', 'IMAGENAME eq tally*', '/FO', 'CSV', '/NH'], { windowsHide: true }, (e, out) => {
      if (e) return resolve(null);
      const rows = out.trim().split(/\r?\n/).filter((l) => /^"tally/i.test(l));
      if (!rows.length) return resolve({ running: false, memMb: 0 });
      const memMb = Math.max(...rows.map((r) => {
        const cols = r.split('","').map((s) => s.replace(/"/g, ''));
        return Math.round((parseInt((cols[4] || '').replace(/[^0-9]/g, ''), 10) || 0) / 1024);
      }));
      resolve({ running: true, memMb });
    });
  });
}

const beep = () => {
  try { process.stdout.write('\x07'); } catch {}
};

/**
 * Block until Tally is up AND the company is open. Prints clear instructions.
 * Called after ANY transport failure and before heavy phases.
 */
async function waitForTally(company, { reason = 'not responding' } = {}) {
  const t0 = Date.now();
  let delay = 5000, warned = false;
  while (Date.now() - t0 < CFG.maxWaitMin * 60000) {
    const p = await probeTally();
    if (p.alive && (!company || p.companies.some((c) => c.toLowerCase() === company.toLowerCase()))) {
      const settleMs = process.env.TALLY_SETTLE_MS ? Number(process.env.TALLY_SETTLE_MS) : 10000;
      await sleep(settleMs); // settle time after restart
      if (warned) console.log('\n✅ Tally is back. Resuming sync...');
      return true;
    }
    if (!warned) {
      beep();
      console.log(
        `\n⚠️  Tally is ${reason}${p.alive ? ' (company not open)' : ' (probably crashed)'}.\n` +
        `   1) Start / restart TallyPrime\n   2) Open company "${company || ''}"\n   3) Stay on Gateway of Tally (no report open)\n` +
        `   The sync resumes automatically from the last good window.`
      );
      warned = true;
    }
    await sleep(delay);
    delay = Math.min(Math.round(delay * 1.5), 30000);
  }
  throw new Error(`Tally did not come back within ${CFG.maxWaitMin} minutes`);
}

/** Memory guard: if Tally's RAM is too high, ask for a restart before continuing. */
async function ensureTallyHealthy(company, log = () => {}) {
  const proc = await tallyProcess();
  if (proc && !proc.running) return waitForTally(company, { reason: 'not running' });
  if (proc && proc.memMb >= CFG.memMaxMb) {
    beep();
    console.log(`\n⚠️  Tally is using ${proc.memMb} MB RAM. Restart Tally and reopen "${company}" to avoid a crash.`);
    log(`tally memory high: ${proc.memMb}MB`);
    const t0 = Date.now();
    while (Date.now() - t0 < CFG.maxWaitMin * 60000) {
      await sleep(10000);
      const p = await tallyProcess();
      if (!p || (p.running && p.memMb < CFG.memMaxMb * 0.7)) break;
    }
    return waitForTally(company);
  }
  if (proc && proc.memMb >= CFG.memWarnMb) log(`tally memory warning: ${proc.memMb}MB`);
  const p = await probeTally();
  if (!p.alive) return waitForTally(company);
  return true;
}

module.exports = {
  CFG,
  sleep,
  esc,
  TallyDownError,
  TallyLogicalError,
  tallyRequest,
  withTallyThrottle,
  probeTally,
  tallyProcess,
  waitForTally,
  ensureTallyHealthy,
  sanitizeXml,
  logReq,
};
