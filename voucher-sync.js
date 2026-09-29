'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const sax = require('sax');
const { tallyRequest, sanitizeXml, waitForTally } = require('./tally-client');
const { voucherHeadersXml, voucherDetailXml, voucherOneXml } = require('./voucher-requests');
const { runAdaptive, createWindowController, addDays } = require('./window-runner');

const QUARANTINE_FILE = path.join(os.homedir(), 'tally-agent-quarantine.json');

function ymd(d) {
  if (!d) return '';
  if (typeof d === 'string') {
    const clean = d.replace(/[^0-9]/g, '');
    if (clean.length === 8) return clean;
  }
  const dt = d instanceof Date ? d : new Date(d);
  const yr = dt.getFullYear();
  const mo = String(dt.getMonth() + 1).padStart(2, '0');
  const dy = String(dt.getDate()).padStart(2, '0');
  return `${yr}${mo}${dy}`;
}

function loadQuarantine() {
  try {
    if (fs.existsSync(QUARANTINE_FILE)) {
      return JSON.parse(fs.readFileSync(QUARANTINE_FILE, 'utf8')) || [];
    }
  } catch {}
  return [];
}

function saveQuarantine(list) {
  try {
    fs.writeFileSync(QUARANTINE_FILE, JSON.stringify(list, null, 2));
  } catch {}
}

const quarantineManager = {
  get: loadQuarantine,
  save: saveQuarantine,
  add(item) {
    const list = loadQuarantine();
    const existingIdx = list.findIndex(
      (q) =>
        q.company === item.company &&
        ((item.guid && q.guid === item.guid) ||
          (item.masterId && q.masterId === item.masterId) ||
          (q.day === item.day && !q.masterId && !item.masterId))
    );
    const entry = {
      ...item,
      firstSeen: existingIdx >= 0 ? list[existingIdx].firstSeen : new Date().toISOString(),
      lastAttempt: new Date().toISOString(),
      attempts: (existingIdx >= 0 ? (list[existingIdx].attempts || 1) : 0) + 1,
    };
    if (existingIdx >= 0) {
      list[existingIdx] = entry;
    } else {
      list.push(entry);
    }
    saveQuarantine(list);
  },
  remove(matchFn) {
    const list = loadQuarantine();
    const filtered = list.filter((item) => !matchFn(item));
    saveQuarantine(filtered);
    return filtered;
  },
  count(company) {
    const list = loadQuarantine();
    if (!company) return list.length;
    return list.filter((q) => !q.company || q.company.toLowerCase() === company.toLowerCase()).length;
  }
};

function parseHeaders(xml) {
  const out = [];
  let cur = null;
  let tag = null;
  const p = sax.parser(true, { trim: true });

  p.onopentag = (n) => {
    if (n.name === 'VOUCHER') cur = {};
    else if (cur) tag = n.name;
  };

  p.ontext = (t) => {
    if (cur && tag && t.trim()) {
      cur[tag] = (cur[tag] || '') + t.trim();
    }
  };

  p.onclosetag = (n) => {
    if (n === 'VOUCHER' && cur) {
      const rawDate = cur.DATE || cur.REFERENCEDATE || '';
      let dateNorm = '';
      if (/^\d{8}$/.test(rawDate)) {
        dateNorm = rawDate;
      } else if (/^\d{4}-\d{2}-\d{2}$/.test(rawDate)) {
        dateNorm = rawDate.replace(/-/g, '');
      } else {
        dateNorm = ymd(rawDate);
      }

      out.push({
        guid: cur.GUID,
        masterId: cur.MASTERID,
        alterId: String(cur.ALTERID || ''),
        date: dateNorm,
        type: cur.VOUCHERTYPENAME || cur.VOUCHERTYPE || '',
        cancelled: cur.ISCANCELLED === 'Yes' || cur.ISDELETED === 'Yes',
        optional: cur.ISOPTIONAL === 'Yes',
      });
      cur = null;
    }
    tag = null;
  };

  p.write(sanitizeXml(xml)).close();
  return out.filter((h) => h.guid);
}

/**
 * DB contract:
 *  db.getVoucherAlterIds(companyGuid, fromDate, toDate) -> Map<guid, alterId>
 *  db.markVouchersDeleted(companyGuid, guids[])
 *  saveVouchersFromStream(ctx, res) -> existing SAX + upsert path, used as the tallyRequest consumer
 */
function makeWork(ctx, db, saveVouchersFromStream) {
  return async function work(from, to) {
    const f = ymd(from);
    const t = ymd(to);

    // ---- Phase 1: headers ----
    const xml = await tallyRequest(voucherHeadersXml(ctx.company, f, t), {
      kind: 'voucher-headers',
      meta: { from: f, to: t },
    });
    const headers = parseHeaders(xml);

    const known = await db.getVoucherAlterIds(ctx.companyGuid, from, to);
    const seen = new Set(headers.map((h) => h.guid));
    const changed = headers.filter((h) => known.get(h.guid) !== h.alterId);
    const gone = [...known.keys()].filter((g) => !seen.has(g));

    if (headers.length > 0 && gone.length > 0) {
      await db.markVouchersDeleted(ctx.companyGuid, gone);
    }

    if (ctx.stats) {
      ctx.stats.headers = (ctx.stats.headers || 0) + headers.length;
      ctx.stats.changed = (ctx.stats.changed || 0) + changed.length;
    }

    if (typeof ctx.onHeaders === 'function') {
      ctx.onHeaders(headers, changed);
    }

    if (!changed.length) {
      // Nothing changed in this window; stop after Phase 1!
      return;
    }

    // ---- Phase 2: details ----
    const days = [...new Set(changed.map((h) => h.date).filter(Boolean))];
    const fromDate = from instanceof Date ? from : new Date(from);
    const toDate = to instanceof Date ? to : new Date(to);
    const windowDays = Math.round((toDate.getTime() - fromDate.getTime()) / 86400000) + 1;

    const ranges = days.length >= 0.6 * windowDays ? [[f, t]] : days.map((d) => [d, d]);

    for (const [rf, rt] of ranges) {
      await tallyRequest(voucherDetailXml(ctx.company, rf, rt), {
        kind: 'voucher-detail',
        meta: { from: rf, to: rt, changed: changed.length },
        idleMs: +process.env.VOUCHER_STREAM_TIMEOUT_MS || 600000,
        consumer: (res) => saveVouchersFromStream(ctx, res),
      });
    }
  };
}

/** Voucher-by-voucher isolation for a day that keeps crashing Tally. */
function makeIsolate(ctx, db, saveVouchersFromStream, quarantine = quarantineManager) {
  return async function isolate(day) {
    const d = ymd(day);
    let headers;
    try {
      headers = parseHeaders(
        await tallyRequest(voucherHeadersXml(ctx.company, d, d), {
          kind: 'isolate-headers',
          meta: { day: d },
        })
      );
    } catch (e) {
      quarantine.add({
        company: ctx.company,
        day: d,
        reason: 'headers failed: ' + e.message,
      });
      return { ok: false };
    }

    let allOk = true;
    for (const h of headers) {
      try {
        await tallyRequest(voucherOneXml(ctx.company, d, h.masterId), {
          kind: 'isolate-one',
          meta: { day: d, masterId: h.masterId },
          consumer: (res) => saveVouchersFromStream(ctx, res),
        });
      } catch (e) {
        allOk = false;
        quarantine.add({
          company: ctx.company,
          day: d,
          masterId: h.masterId,
          guid: h.guid,
          type: h.type,
          reason: e.message || String(e),
        });
        const msg = `QUARANTINED voucher masterId=${h.masterId} day=${d} type=${h.type}`;
        if (typeof ctx.log === 'function') ctx.log(msg);
        console.log(`\n⚠️  ${msg}`);
        await waitForTally(ctx.company);
      }
    }
    return { ok: allOk };
  };
}

module.exports = {
  ymd,
  parseHeaders,
  makeWork,
  makeIsolate,
  runAdaptive,
  createWindowController,
  quarantineManager,
};
