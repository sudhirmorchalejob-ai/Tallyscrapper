'use strict';
const assert = require('assert');
const { parseHeaders } = require('../voucher-sync');
const { createWindowController, runAdaptive, addDays, span } = require('../window-runner');
const { voucherHeadersXml, voucherDetailXml, voucherOneXml } = require('../voucher-requests');
const { TallyDownError, TallyLogicalError } = require('../tally-client');

console.log('--- 1. Testing voucher-requests ---');
{
  const hXml = voucherHeadersXml('TEST CO', '20230701', '20230705');
  assert(hXml.includes('<ID>VchHeaders</ID>'), 'has VchHeaders ID');
  assert(hXml.includes('20230701'), 'has from date');
  assert(hXml.includes('20230705'), 'has to date');
  assert(hXml.includes('TEST CO'), 'has company');

  const dXml = voucherDetailXml('TEST CO', '20230701', '20230701');
  assert(dXml.includes('<ID>VchDetail</ID>'), 'has VchDetail ID');
  assert(dXml.includes('AllLedgerEntries'), 'has AllLedgerEntries');

  const oXml = voucherOneXml('TEST CO', '20230715', 12345);
  assert(oXml.includes('<ID>VchOne</ID>'), 'has VchOne ID');
  assert(oXml.includes('$MasterID = 12345'), 'has MasterID formula');
  console.log('ok: voucher-requests XML generation');
}

console.log('--- 2. Testing parseHeaders ---');
{
  const sample = `<ENVELOPE>
    <VOUCHER>
      <GUID>g-101</GUID>
      <MASTERID>101</MASTERID>
      <ALTERID>5</ALTERID>
      <DATE>20230701</DATE>
      <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>
      <ISCANCELLED>No</ISCANCELLED>
      <ISOPTIONAL>No</ISOPTIONAL>
    </VOUCHER>
    <VOUCHER>
      <GUID>g-102</GUID>
      <MASTERID>102</MASTERID>
      <ALTERID>2</ALTERID>
      <DATE>20230702</DATE>
      <VOUCHERTYPENAME>Purchase</VOUCHERTYPENAME>
      <ISCANCELLED>Yes</ISCANCELLED>
      <ISOPTIONAL>No</ISOPTIONAL>
    </VOUCHER>
  </ENVELOPE>`;

  const headers = parseHeaders(sample);
  assert.strictEqual(headers.length, 2, 'parsed 2 headers');
  assert.strictEqual(headers[0].guid, 'g-101');
  assert.strictEqual(headers[0].masterId, '101');
  assert.strictEqual(headers[0].alterId, '5');
  assert.strictEqual(headers[0].date, '20230701');
  assert.strictEqual(headers[0].type, 'Sales');
  assert.strictEqual(headers[0].cancelled, false);
  assert.strictEqual(headers[1].cancelled, true);
  console.log('ok: parseHeaders extraction');
}

console.log('--- 3. Testing createWindowController ---');
{
  const ctrl = createWindowController({ configuredDays: 7, maxDays: 7 });
  assert.strictEqual(ctrl.size(), 7, 'starts at 7');
  ctrl.onCrash();
  assert.strictEqual(ctrl.size(), 3, 'halved to 3 after crash');
  ctrl.onCrash();
  assert.strictEqual(ctrl.size(), 1, 'halved to 1 after 2nd crash');
  ctrl.onCrash();
  assert.strictEqual(ctrl.size(), 1, 'floor at 1');

  for (let i = 0; i < 20; i++) ctrl.onOk();
  assert.strictEqual(ctrl.size(), 2, 'grows to 2 after 20 ok');
  for (let i = 0; i < 20; i++) ctrl.onOk();
  assert.strictEqual(ctrl.size(), 3, 'grows to 3 after another 20 ok');
  console.log('ok: createWindowController halving and growth');
}

console.log('--- 4. Testing runAdaptive bisection & isolation ---');
(async () => {
  const ctrl = createWindowController({ configuredDays: 4, maxDays: 7 });
  const failedRanges = [];
  const successfulRanges = [];
  const isolatedDays = [];

  const fmt = (d) => {
    const yr = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const dy = String(d.getDate()).padStart(2, '0');
    return `${yr}-${mo}-${dy}`;
  };

  // Simulate failure when window span > 1 day, or if day is 2023-07-03
  const work = async (from, to) => {
    const s = span(from, to);
    const fromStr = fmt(from);
    if (s > 1) {
      failedRanges.push(`${fromStr} (${s}d)`);
      throw new Error(`Window too wide: ${s} days`);
    }
    if (fromStr === '2023-07-03') {
      failedRanges.push(`${fromStr} (corrupt day)`);
      throw new Error('Corrupt day');
    }
    successfulRanges.push(fromStr);
  };

  const isolate = async (day) => {
    const dStr = fmt(day);
    isolatedDays.push(dStr);
    // 2023-07-03 has a corrupt voucher so isolate returns ok: false
    return { ok: false };
  };

  const ctx = {
    company: 'TEST CO',
    log: () => {},
    controller: ctrl,
    ensureTallyHealthy: async () => true,
    waitForTally: async () => true,
  };

  const from = new Date(2023, 6, 1); // 2023-07-01
  const to = new Date(2023, 6, 4);   // 2023-07-04 (4 days span)

  const res = await runAdaptive(ctx, from, to, work, isolate);

  assert.strictEqual(failedRanges.length >= 2, true, 'wide windows and corrupt day failed');
  assert.strictEqual(successfulRanges.includes('2023-07-01'), true, '2023-07-01 succeeded');
  assert.strictEqual(successfulRanges.includes('2023-07-02'), true, '2023-07-02 succeeded');
  assert.strictEqual(successfulRanges.includes('2023-07-04'), true, '2023-07-04 succeeded');
  assert.strictEqual(isolatedDays.includes('2023-07-03'), true, '2023-07-03 was isolated');
  assert.strictEqual(res.quarantined.length, 1, '1 quarantined day');
  console.log('ok: runAdaptive adaptive bisection and quarantine');

  console.log('\nALL UNIT TESTS PASSED!');
})();
