'use strict';
const { esc } = require('./tally-client');

const envelope = (id, company, from, to, collection, extra = '') => `<ENVELOPE><HEADER><VERSION>1</VERSION>
<TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>${id}</ID></HEADER><BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
<SVCURRENTCOMPANY>${esc(company)}</SVCURRENTCOMPANY>
<SVFROMDATE TYPE="Date">${from}</SVFROMDATE><SVTODATE TYPE="Date">${to}</SVTODATE></STATICVARIABLES>
<TDL><TDLMESSAGE>${collection}${extra}</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>`;

const HEADER_FIELDS = 'GUID, MasterID, AlterID, Date, VoucherTypeName, IsCancelled, IsOptional';

const DETAIL_FIELDS = `GUID, MasterID, AlterID, Date, VoucherTypeName, VoucherNumber, Reference,
PartyLedgerName, IsCancelled, IsOptional, Narration,
AllLedgerEntries.List, LedgerEntries.List, AllInventoryEntries.List`;

const voucherHeadersXml = (company, from, to) =>
  envelope(
    'VchHeaders',
    company,
    from,
    to,
    `<COLLECTION NAME="VchHeaders"><TYPE>Voucher</TYPE><FETCH>${HEADER_FIELDS}</FETCH></COLLECTION>`
  );

const voucherDetailXml = (company, from, to) =>
  envelope(
    'VchDetail',
    company,
    from,
    to,
    `<COLLECTION NAME="VchDetail"><TYPE>Voucher</TYPE><FETCH>${DETAIL_FIELDS}</FETCH></COLLECTION>`
  );

// Single voucher by MasterID (isolation of a corrupt voucher). from/to = that day.
const voucherOneXml = (company, day, masterId) =>
  envelope(
    'VchOne',
    company,
    day,
    day,
    `<COLLECTION NAME="VchOne"><TYPE>Voucher</TYPE><FETCH>${DETAIL_FIELDS}</FETCH><FILTER>OneF</FILTER></COLLECTION>`,
    `<SYSTEM TYPE="Formulae" NAME="OneF">$MasterID = ${Number(masterId)}</SYSTEM>`
  );

module.exports = {
  envelope,
  HEADER_FIELDS,
  DETAIL_FIELDS,
  voucherHeadersXml,
  voucherDetailXml,
  voucherOneXml,
};
