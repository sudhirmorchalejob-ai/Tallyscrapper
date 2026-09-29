'use strict';
const { TallyLogicalError, ensureTallyHealthy, waitForTally } = require('./tally-client');

const DAY = 86400000;
const addDays = (d, n) => new Date(d.getTime() + n * DAY);
const span = (a, b) => Math.round((b.getTime() - a.getTime()) / DAY) + 1;

function createWindowController({ configuredDays = 1, maxDays = 7 } = {}) {
  let cur = Math.max(1, Math.min(configuredDays, maxDays));
  let okStreak = 0;
  return {
    size: () => cur,
    onCrash: () => {
      cur = Math.max(1, Math.floor(cur / 2));
      okStreak = 0;
    },
    onOk: () => {
      if (++okStreak >= 20 && cur < Math.min(configuredDays, maxDays)) {
        cur++;
        okStreak = 0;
      }
    },
  };
}

/**
 * work(from, to)      -> Promise, idempotent
 * isolate(day)        -> Promise<{ok:boolean}>  voucher-by-voucher fallback for a bad day
 */
async function runAdaptive(ctx, from, to, work, isolate, tries = 0) {
  const { company, log = () => {}, controller } = ctx;
  const healthFn = typeof ctx.ensureTallyHealthy === 'function' ? ctx.ensureTallyHealthy : ensureTallyHealthy;
  const waitFn = typeof ctx.waitForTally === 'function' ? ctx.waitForTally : waitForTally;

  try {
    await healthFn(company, log);
    await work(from, to);
    if (controller && typeof controller.onOk === 'function') {
      controller.onOk();
    }
    return { ok: true, quarantined: [] };
  } catch (err) {
    const fromStr = from instanceof Date ? from.toISOString().slice(0, 10) : String(from);
    const toStr = to instanceof Date ? to.toISOString().slice(0, 10) : String(to);
    log(`window ${fromStr}..${toStr} failed: ${err.name || 'Error'}: ${err.message || String(err)}`);

    if (err instanceof TallyLogicalError) {
      await waitFn(company, { reason: 'reporting an error: ' + err.message });
      if (tries < 3) return runAdaptive(ctx, from, to, work, isolate, tries + 1);
      throw err;
    }

    if (controller && typeof controller.onCrash === 'function') {
      controller.onCrash();
    }

    await waitFn(company); // waits for restart if it crashed

    const fromDate = from instanceof Date ? from : new Date(from);
    const toDate = to instanceof Date ? to : new Date(to);
    const n = span(fromDate, toDate);

    if (n > 1) {
      const mid = addDays(fromDate, Math.ceil(n / 2) - 1);
      const a = await runAdaptive(ctx, fromDate, mid, work, isolate);
      const b = await runAdaptive(ctx, addDays(mid, 1), toDate, work, isolate);
      return { ok: a.ok && b.ok, quarantined: [...(a.quarantined || []), ...(b.quarantined || [])] };
    }

    if (tries < 1) {
      // one more try for a single day
      return runAdaptive(ctx, fromDate, toDate, work, isolate, tries + 1);
    }

    // Single day failed twice -> voucher-by-voucher isolation
    if (typeof isolate === 'function') {
      const r = await isolate(fromDate);
      return { ok: !!(r && r.ok), quarantined: r && r.ok ? [] : [fromDate] };
    }

    return { ok: false, quarantined: [fromDate] };
  }
}

module.exports = {
  createWindowController,
  runAdaptive,
  addDays,
  span,
};
