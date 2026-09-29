'use strict';
const assert = require('assert');
const path = require('path');
const os = require('os');
const fs = require('fs');

process.env.TALLY_PORT = '9100';
process.env.MOCK_TALLY_PORT = '9100';
process.env.MOCK_DOWN_MS = '100';
process.env.TALLY_SETTLE_MS = '100';
process.env.TALLY_MIN_GAP_MS = '10';
process.env.TALLY_TIMEOUT_MS = '3000';
process.env.VOUCHER_WINDOW_DAYS = '7';

const { start, stop } = require('./mock-tally.cjs');
const { tallyRequest, waitForTally } = require('../tally-client');
const { createWindowController, runAdaptive, addDays } = require('../window-runner');
const { ymd, parseHeaders, makeWork, makeIsolate, quarantineManager } = require('../voucher-sync');

// Clear existing quarantine for test clean slate
const qFile = path.join(os.homedir(), 'tally-agent-quarantine.json');
try { if (fs.existsSync(qFile)) fs.unlinkSync(qFile); } catch {}

start();

async function runTest() {
  console.log('--- Integration test: mock Tally on port 9100 ---');

  const controller = createWindowController({ configuredDays: 7, maxDays: 7 });
  const inMemoryDb = new Map(); // guid -> alterId
  const savedVouchers = [];

  const dbOps = {
    getVoucherAlterIds: async (cGuid, from, to) => inMemoryDb,
    markVouchersDeleted: async (cGuid, guids) => {
      for (const g of guids) inMemoryDb.delete(g);
    }
  };

  const saveVouchersFromStream = async (ctx, res) => {
    let body = '';
    for await (const chunk of res) body += chunk.toString();
    const matches = [...body.matchAll(/<VOUCHER>[\s\S]*?<\/VOUCHER>/gi)];
    for (const m of matches) {
      const g = m[0].match(/<GUID>([^<]+)<\/GUID>/i)?.[1];
      const a = m[0].match(/<ALTERID>([^<]+)<\/ALTERID>/i)?.[1];
      if (g) {
        inMemoryDb.set(g, a || '1');
        savedVouchers.push(g);
      }
    }
  };

  const logs = [];
  const syncCtx = {
    company: 'TEST CO',
    companyGuid: 'test-guid-1',
    controller,
    stats: { headers: 0, changed: 0 },
    log: (msg) => logs.push(msg)
  };

  const work = makeWork(syncCtx, dbOps, saveVouchersFromStream);
  const isolate = makeIsolate(syncCtx, dbOps, saveVouchersFromStream, quarantineManager);

  // Sync 2023-07-01 to 2023-07-31
  const from = new Date(2023, 6, 1);
  const to = new Date(2023, 6, 31);

  console.log('Running runAdaptive from 2023-07-01 to 2023-07-31 (configured window: 7 days)...');
  const res = await runAdaptive(syncCtx, from, to, work, isolate);

  console.log('Sync finished with result:', res);
  assert(res.quarantined.length > 0, 'Poison day 2023-07-15 must be quarantined');

  const qList = quarantineManager.get();
  console.log('Quarantine entries:', qList);
  assert(qList.length >= 1, 'At least 1 item quarantined in file');
  const poisonItem = qList.find(q => q.masterId === '2023071599');
  assert(poisonItem, 'Poison voucher masterId 2023071599 was quarantined');

  console.log('Saved vouchers count:', savedVouchers.length, savedVouchers);
  assert(savedVouchers.includes('g-2023071501'), 'Good voucher on poison day was saved!');
  assert(!savedVouchers.includes('g-2023071599'), 'Poison voucher was not saved in DB!');
  assert(savedVouchers.includes('g-20230701'), 'Day 1 voucher was saved');
  assert(savedVouchers.includes('g-20230729'), 'Final window voucher was saved');

  console.log('✅ Integration test PASSED! Mock crash, auto-resume, window bisection and quarantine verified.');
  stop();
  process.exit(0);
}

runTest().catch((err) => {
  console.error('❌ Integration test FAILED:', err);
  stop();
  process.exit(1);
});
