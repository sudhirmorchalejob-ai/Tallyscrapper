#!/usr/bin/env nodeimport { createRequire as __WEBPACK_EXTERNAL_createRequire } from "module";
import * as __WEBPACK_EXTERNAL_MODULE_axios__ from "axios";
import * as __WEBPACK_EXTERNAL_MODULE_xml2js__ from "xml2js";
/******/ // The require scope
/******/ var __nccwpck_require__ = {};
/******/ 
/************************************************************************/
/******/ /* webpack/runtime/define property getters */
/******/ (() => {
/******/ 	// define getter functions for harmony exports
/******/ 	__nccwpck_require__.d = (exports, definition) => {
/******/ 		for(var key in definition) {
/******/ 			if(__nccwpck_require__.o(definition, key) && !__nccwpck_require__.o(exports, key)) {
/******/ 				Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
/******/ 			}
/******/ 		}
/******/ 	};
/******/ })();
/******/ 
/******/ /* webpack/runtime/hasOwnProperty shorthand */
/******/ (() => {
/******/ 	__nccwpck_require__.o = (obj, prop) => (Object.prototype.hasOwnProperty.call(obj, prop))
/******/ })();
/******/ 
/******/ /* webpack/runtime/compat */
/******/ 
/******/ if (typeof __nccwpck_require__ !== 'undefined') __nccwpck_require__.ab = new URL('.', import.meta.url).pathname.slice(import.meta.url.match(/^file:\/\/\/\w:/) ? 1 : 0, -1) + "/";
/******/ 
/************************************************************************/
var __webpack_exports__ = {};

;// CONCATENATED MODULE: external "url"
const external_url_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("url");
;// CONCATENATED MODULE: external "path"
const external_path_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("path");
;// CONCATENATED MODULE: external "os"
const external_os_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("os");
;// CONCATENATED MODULE: external "axios"
var x = (y) => {
	var x = {}; __nccwpck_require__.d(x, y); return x
} 
var y = (x) => (() => (x))
const external_axios_namespaceObject = x({ ["default"]: () => (__WEBPACK_EXTERNAL_MODULE_axios__["default"]) });
;// CONCATENATED MODULE: external "xml2js"
var external_xml2js_x = (y) => {
	var x = {}; __nccwpck_require__.d(x, y); return x
} 
var external_xml2js_y = (x) => (() => (x))
const external_xml2js_namespaceObject = external_xml2js_x({ ["default"]: () => (__WEBPACK_EXTERNAL_MODULE_xml2js__["default"]), ["parseStringPromise"]: () => (__WEBPACK_EXTERNAL_MODULE_xml2js__.parseStringPromise) });
;// CONCATENATED MODULE: external "fs"
const external_fs_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("fs");
;// CONCATENATED MODULE: external "crypto"
const external_crypto_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("crypto");
;// CONCATENATED MODULE: external "readline"
const external_readline_namespaceObject = __WEBPACK_EXTERNAL_createRequire(import.meta.url)("readline");
;// CONCATENATED MODULE: ./agent.js













const agent_filename = (0,external_url_namespaceObject.fileURLToPath)(import.meta.url);
const agent_dirname = external_path_namespaceObject.dirname(agent_filename);

external_axios_namespaceObject["default"].defaults.proxy = false;


function extractAddressValue(addr) {
  if (!addr) return null;

  // ADDRESS as array
  if (Array.isArray(addr)) {
    return addr
      .map(a => (typeof a === "object" ? a._ : a))
      .filter(Boolean)
      .join(" ");
  }

  // ADDRESS as object { _: value }
  if (typeof addr === "object" && addr._) {
    return addr._;
  }

  // ADDRESS as string
  return String(addr);
}

// 🔎 Smart extractors
function normalizeText(value) {
  if (!value) return null;

  // If array → join
  if (Array.isArray(value)) {
    return value.join(" ");
  }

  // If object with "_" (Tally XML pattern)
  if (typeof value === "object" && value._) {
    return value._;
  }

  // If string or number
  return String(value);
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



function askQuestion(query, hideInput = false) {
  const rl = external_readline_namespaceObject.createInterface({
    input: process.stdin,
    output: process.stdout,
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

let ACTIVE_COMPANY_GUID = null;
let AUTH_TOKEN = null;
//const STATE_FILE = "./agent-sync-state.json";


const STATE_FILE = external_path_namespaceObject.join(
  external_os_namespaceObject.homedir(),
  "tally-agent-state.json"
);


function authHeaders() {
  return {
    headers: {
      Authorization: `Bearer ${AUTH_TOKEN}`
    }
  };
}

async function agentLogin() {
  const username = await askQuestion("Enter admin username: ");
  const password = await askQuestion("Enter admin password: ", true);

  const res = await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/auth/login`,
    { username, password }
  );

  AUTH_TOKEN = res.data.token;

  console.log("✅ Agent authenticated as:", username);
}


function loadState() {
  if (!fs.existsSync(STATE_FILE)) {
    return { last_voucher_alterid: 0 };
  }
  return JSON.parse(fs.readFileSync(STATE_FILE));
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

//import "dotenv/config";




// BACKEND URL
const BACKEND_URL =
  process.env.BACKEND_URL || "https://tally-connect-yga1.onrender.com";

// Tally URL
const TALLY_URL = "http://localhost:9000";

// 5 second timer
//const SYNC_INTERVAL = 5000;
// 5 minute timer
const SYNC_INTERVAL = 5 * 60 * 1000; // 5 minutes in ms


function generatePayloadHash(payload) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(payload))
    .digest("hex");
}


// MAIN SYNC FUNCTION


















/* --------------------------
   1. SYNC COMPANIES
--------------------------- */
async function syncCompanies() {
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

  const response = await external_axios_namespaceObject["default"].post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" },
  });

  const jsonData = await (0,external_xml2js_namespaceObject.parseStringPromise)(response.data);

  const companies =
    jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];

  const companyList = [];

  for (let c of companies) {
    const company_guid =
      typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;

    const name =
      typeof c.NAME?.[0] === "string" ? c.NAME[0] : c.NAME?.[0]?._;

    const res = await external_axios_namespaceObject["default"].post(
      `${BACKEND_URL}/company/create`,
      { company_guid, name },
      authHeaders()
    );

    companyList.push({
      company_guid,
      name,
    });
  }

  console.log(
    "🏢 Companies detected from Tally:",
    companyList.map(c => c.name).join(", ")
  );

  return companyList;
}



/* --------------------------
   2. SYNC LEDGERS
--------------------------- */
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
  <SVCOMPANY>${company.name}</SVCOMPANY>
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

  const res = await external_axios_namespaceObject["default"].post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" },
  });

  const parser = new external_xml2js_namespaceObject["default"].Parser({ explicitArray: false });
  const parsed = await parser.parseStringPromise(res.data);

  const ledgers = parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.LEDGER || [];
  const ledgerArray = Array.isArray(ledgers) ? ledgers : [ledgers];
// 🔥 Collect ALL possible text sources

  console.log("Ledgers from Tally:", ledgerArray.length);

  
for (const l of ledgerArray) {
  // 📮 Mailing details address (most users type contact info here)
// 📮 Mailing details address (robust for object OR array)
let mailingAddressText = null;

const mailingRaw = l["MAILINGDETAILS.LIST"];
const mailingArr = Array.isArray(mailingRaw)
  ? mailingRaw
  : mailingRaw
  ? [mailingRaw]
  : [];

for (const m of mailingArr) {
  if (m?.["ADDRESS.LIST"]?.ADDRESS) {
    mailingAddressText = extractAddressValue(
      m["ADDRESS.LIST"].ADDRESS
    );
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

  // Raw values from Tally
  const rawPhone = l.LEDGERPHONE?._ || l.LEDGERPHONE || null;
  const rawEmail = l.EMAIL?._ || l.EMAIL || null;

  // Addresses (both formats)
  const addressText = normalizeText(l.ADDRESS);
let addressListText = null;
if (l["ADDRESS.LIST"]?.ADDRESS) {
  addressListText = extractAddressValue(l["ADDRESS.LIST"].ADDRESS);
}




  // 🔎 ONE unified searchable blob
  const searchBlob = [
    rawPhone,
    rawEmail,
    addressText,
    addressListText,
      mailingAddressText, 
    l.NAME,
    l.PARENT
  ].filter(Boolean).join(" ");

  // 🔥 Smart extraction
  const phone = extractPhone(searchBlob);
  const email = extractEmail(searchBlob);

  const pan =
    typeof l.INCOMETAXNUMBER === "string"
      ? l.INCOMETAXNUMBER
      : l.INCOMETAXNUMBER?._ || null;

const ledgerGuid =
  typeof l.GUID === "string"
    ? l.GUID
    : l.GUID?._ || null;

// 🚨 SKIP SYSTEM / BROKEN LEDGERS
if (!ledgerGuid || !ledgerName) {
  console.log("⏭️ Skipping invalid ledger:", ledgerName);
  continue;
}

await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/ledger/sync`,
  {
    ledger_guid: ledgerGuid,
    company_guid: company.company_guid,
    name: ledgerName,
    parent_group:
      typeof l.PARENT === "string" ? l.PARENT : l.PARENT?._,
    opening_balance: Number(l.OPENINGBALANCE?._ || 0),
    closing_balance: Number(l.CLOSINGBALANCE?._ || 0),
    type: l.ISBILLWISEON === "Yes" ? "Party" : "General",
    phone,
    email,
    pan
  },
  authHeaders()
);


  console.log("Ledger Pull:", ledgerName, phone, email, pan);
}


}




const BATCH_SIZE = 500;



/* --------------------------
   3. SYNC VOUCHERS
--------------------------- */


async function syncVouchers() {
const invoiceItemBatch = [];

const invoiceBatch = [];

  if (!ACTIVE_COMPANY_GUID) {
    console.log("⚠️ No active company. Skipping vouchers.");
    return;
  }
const voucherMap = new Map();
    const activeVoucherGuids = [];


const xmlRequest = `
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
     <SVFROMDATE>20240101</SVFROMDATE>
     <SVTODATE>20251231</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;


  const res = await external_axios_namespaceObject["default"].post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" },
  });

  console.log("RAW DAYBOOK XML:\n", res.data);

  const parsed = await (0,external_xml2js_namespaceObject.parseStringPromise)(res.data);

  const messages =
    parsed?.ENVELOPE?.BODY?.[0]
      ?.IMPORTDATA?.[0]
      ?.REQUESTDATA?.[0]
      ?.TALLYMESSAGE || [];

  const msgArray = Array.isArray(messages) ? messages : [messages];

  console.log("TALLYMESSAGE count:", msgArray.length);

  let voucherCount = 0;
// const activeVoucherGuids = new Set();

  for (const msg of msgArray) {
    if (!msg.VOUCHER) continue;

    const v = msg.VOUCHER[0];
    voucherCount++;

    // 🔹 Voucher GUID
const voucherGuid =
  typeof v.GUID?.[0] === "string" ? v.GUID[0] : v.GUID?.[0]?._;

if (!voucherGuid) {
  console.log(
    "⏭️ Skipping voucher without GUID:",
    v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]
  );
  continue;
}

activeVoucherGuids.push(voucherGuid);


// 🔹 Voucher Date
const voucherDate =
  v.DATE?.[0] ||
  v.REFERENCEDATE?.[0] ||
  null;

// 🔹 Voucher Type (⭐ MUST COME FIRST)
const voucherType =
  v.VOUCHERTYPENAME?.[0] ||
  v.VOUCHERTYPE?.[0] ||
  null;


     if (voucherCount <= 3) {
    console.log("🔎 FULL VOUCHER OBJECT:", JSON.stringify(v, null, 2));
  }


  const isPurchaseInvoice =
  voucherType &&
  voucherType.toLowerCase().includes("purchase");
console.log("📘 Voucher Type:", voucherType);


if (isPurchaseInvoice) {
  const invoiceGuid =
    typeof v.GUID?.[0] === "string" ? v.GUID[0] : v.GUID?.[0]?._;

  const invoiceDate = v.DATE?.[0] || null;
  const invoiceNo = v.VOUCHERNUMBER?.[0] || null;
  const partyName = v.PARTYLEDGERNAME?.[0] || null;

  const ledgerEntries = v["ALLLEDGERENTRIES.LIST"] || [];
  const entryArray = Array.isArray(ledgerEntries)
    ? ledgerEntries
    : [ledgerEntries];

  let totalAmount = 0;

  for (const e of entryArray) {
    totalAmount += Math.abs(Number(e.AMOUNT?.[0] || 0));
  }

  // 🔹 Save Purchase Invoice Header
await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/invoice/sync`,
  {
    invoice_guid: invoiceGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    invoice_no: invoiceNo,
    invoice_date: invoiceDate,
    invoice_type: "Purchase", // or "Sales"
    party_name: partyName,
    total_amount: totalAmount,
  },
  authHeaders()
);


  // 🔥 CREATE SALES ORDER FROM SALES INVOICE

invoiceBatch.push({
   // admin_id: ADMIN_ID,          // 🔥 ADD THIS
  invoice_guid: invoiceGuid,
  company_guid: ACTIVE_COMPANY_GUID,
  invoice_no: invoiceNo,
  invoice_date: invoiceDate,
  invoice_type: "Purchase",
  party_name: partyName,
  total_amount: totalAmount,
});
if (invoiceBatch.length >= BATCH_SIZE) {
  await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/invoice/bulk-sync`,
  invoiceBatch,
  authHeaders()
);

  invoiceBatch.length = 0;
}


  // 🔹 Save Line Items
const purchaseItems = v["INVENTORYENTRIES.LIST"] || [];
const purchaseItemArray = Array.isArray(purchaseItems)
  ? purchaseItems
  : [purchaseItems];


 for (const i of purchaseItemArray) {
  await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/invoice-item/sync`,
  {
    invoice_guid: invoiceGuid,
    item_name: i.STOCKITEMNAME?.[0] || i.LEDGERNAME?.[0] || null,
    quantity: parseTallyNumber(i.ACTUALQTY?.[0]),
    rate: Number(i.RATE?.[0]?.replace(/[^0-9.-]/g, "") || 0),
    amount: Math.abs(Number(i.AMOUNT?.[0] || 0)),
  },
  authHeaders()
);


  invoiceItemBatch.push({
  //    admin_id: ADMIN_ID,

    invoice_guid: invoiceGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    item_name: i.STOCKITEMNAME?.[0] || i.LEDGERNAME?.[0] || null,
quantity: parseTallyNumber(i.ACTUALQTY?.[0]),
    rate: Number(i.RATE?.[0]?.replace(/[^0-9.-]/g, "") || 0),
    amount: Math.abs(Number(i.AMOUNT?.[0] || 0)),
  });

  if (invoiceItemBatch.length >= BATCH_SIZE) {
    await external_axios_namespaceObject["default"].post(
      `${BACKEND_URL}/invoice-item/bulk-sync`,
      invoiceItemBatch
    );
    invoiceItemBatch.length = 0;
  }
}


  console.log(`🧾 Purchase Invoice synced → ${invoiceNo}`);


}


   
const isSalesInvoice =
  voucherType &&
  (
    voucherType.toLowerCase().includes("invoice") ||
    voucherType.toLowerCase().includes("sales")
  );

if (isSalesInvoice) {
  const invoiceGuid =
    typeof v.GUID?.[0] === "string" ? v.GUID[0] : v.GUID?.[0]?._;

  const invoiceDate = v.DATE?.[0] || null;
  const invoiceNo = v.VOUCHERNUMBER?.[0] || null;
  const partyName = v.PARTYLEDGERNAME?.[0] || null;

  const ledgerEntries = v["ALLLEDGERENTRIES.LIST"] || [];
  const entryArray = Array.isArray(ledgerEntries)
    ? ledgerEntries
    : [ledgerEntries];

  let totalAmount = 0;

  for (const e of entryArray) {
    totalAmount += Math.abs(Number(e.AMOUNT?.[0] || 0));
  }

  // 🔹 Save Invoice Header
 await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/invoice/sync`,
  {
    invoice_guid: invoiceGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    invoice_no: invoiceNo,
    invoice_date: invoiceDate,
    invoice_type: "Sales",
    party_name: partyName,
    total_amount: totalAmount,
  },
  authHeaders()
);


  invoiceBatch.push({
  //    admin_id: ADMIN_ID,              // 🔥 ADD THIS LINE
  invoice_guid: invoiceGuid,
  company_guid: ACTIVE_COMPANY_GUID,
  invoice_no: invoiceNo,
  invoice_date: invoiceDate,
  invoice_type: "Sales",
  party_name: partyName,
  total_amount: totalAmount,
});
if (invoiceBatch.length >= BATCH_SIZE) {
  await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/invoice/bulk-sync`,
    invoiceBatch,authHeaders()
  );
  invoiceBatch.length = 0;
}

   // ✅ CREATE SALES ORDER FROM SALES INVOICE
 await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/sales-order/sync`,
  {
    order_guid: invoiceGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    order_no: invoiceNo,
    order_date: invoiceDate,
    order_type: voucherType,
    party_name: partyName,
    total_amount: totalAmount,
    status: "Pending",
    due_date: null
  },
  authHeaders()
);


  console.log(`📦 Sales Order created → ${invoiceNo}`);

  // 🔹 Save Invoice Line Items
  const salesItems = v["INVENTORYENTRIES.LIST"] || [];
const salesItemArray = Array.isArray(salesItems)
  ? salesItems
  : [salesItems];


for (const i of salesItemArray) {
 await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/invoice-item/sync`,
  {
    invoice_guid: invoiceGuid,
    item_name: i.STOCKITEMNAME?.[0] || null,
    quantity: parseTallyNumber(i.ACTUALQTY?.[0]),
    rate: Number(i.RATE?.[0]?.replace(/[^0-9.-]/g, "") || 0),
    amount: Math.abs(Number(i.AMOUNT?.[0] || 0)),
  },
  authHeaders()
);


  invoiceItemBatch.push({
    //  admin_id: ADMIN_ID,              // 🔥 ADD THIS
    invoice_guid: invoiceGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    item_name: i.STOCKITEMNAME?.[0] || null,
quantity: parseTallyNumber(i.ACTUALQTY?.[0]),
    rate: Number(i.RATE?.[0]?.replace(/[^0-9.-]/g, "") || 0),
    amount: Math.abs(Number(i.AMOUNT?.[0] || 0)),
  });

  if (invoiceItemBatch.length >= BATCH_SIZE) {
    await external_axios_namespaceObject["default"].post(
      `${BACKEND_URL}/invoice-item/bulk-sync`,
      invoiceItemBatch,authHeaders()
    );
    invoiceItemBatch.length = 0;
  }
}


  console.log(`🧾 Sales Invoice synced → ${invoiceNo}`);



}


const referenceNo =
  v.REFERENCE?.[0] ||
  v.REFERENCENUMBER?.[0] ||
  null;


    const entries = v["ALLLEDGERENTRIES.LIST"] || [];
    const entryArray = Array.isArray(entries) ? entries : [entries];
//const referenceNo = v.REFERENCE?.[0] || null;
    for (const e of entryArray) {
      const isDebit = e.ISDEEMEDPOSITIVE?.[0] === "No";

      if (!voucherGuid) continue;

   if (!voucherMap.has(voucherGuid)) {
  voucherMap.set(voucherGuid, {
  //  admin_id: ADMIN_ID,
    voucher_guid: voucherGuid,
    company_guid: ACTIVE_COMPANY_GUID,
    voucher_date: voucherDate,
    voucher_type: voucherType,
reference_no: referenceNo,
  party_name: v.PARTYLEDGERNAME?.[0] || null,

    entries: []
  });
}

voucherMap.get(voucherGuid).entries.push({
  ledger_name: e.LEDGERNAME?.[0],
  amount: Math.abs(Number(e.AMOUNT?.[0] || 0)),
  is_debit: isDebit
});








      console.log(
        `Entry synced → ${e.LEDGERNAME?.[0]} : ${isDebit ? "DR" : "CR"}`
      );
    }
  }
  // ✅ FINAL VOUCHER SYNC (CORRECT PLACE)
for (const voucher of voucherMap.values()) {
  await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/voucher-entry/sync`,
    voucher,authHeaders()
  );
}

// 🔴 Mark deleted vouchers as inactive


console.log("🧹 Inactive vouchers cleaned");


  console.log("Vouchers from Tally:", voucherCount);
    // 🔒 Mark deleted vouchers as inactive
  if (activeVoucherGuids.length > 0) {
await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/voucher-entry/mark-inactive`,
  {
    company_guid: ACTIVE_COMPANY_GUID,
    activeVoucherGuids
  },
  authHeaders()
);



  }




if (invoiceBatch.length > 0) {
  await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/invoice/bulk-sync`,
    invoiceBatch,authHeaders()
  );
}

if (invoiceItemBatch.length > 0) {
  await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/invoice-item/bulk-sync`,
    invoiceItemBatch,authHeaders()
  );
}
}



/* --------------------------
   4. SYNC BILLS
--------------------------- */
/* --------------------------
   4. SYNC BILLS
--------------------------- */
async function syncBills() {
  if (!ACTIVE_COMPANY_GUID) {
    console.log("⚠️ No active company. Skipping bills sync.");
    return;
  }

  console.log("🧾 Syncing Bills Receivable (Flat Report)...");

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
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const res = await external_axios_namespaceObject["default"].post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" },
  });

  const parsed = await (0,external_xml2js_namespaceObject.parseStringPromise)(res.data);

  const billFixedArr = parsed?.ENVELOPE?.BILLFIXED || [];
  const bills = Array.isArray(billFixedArr)
    ? billFixedArr
    : [billFixedArr];

  let synced = 0;

  for (const bill of bills) {
    const ledgerName = bill.BILLPARTY?.[0];
    const billName   = bill.BILLREF?.[0];
    const billDate   = bill.BILLDATE?.[0];

    if (!ledgerName || !billName) {
      console.log("⏭️ Skipping invalid bill:", bill);
      continue;
    }

    const payload = {
      company_guid: ACTIVE_COMPANY_GUID,
      ledger_name: ledgerName,
      bill_name: billName,
      bill_date: billDate,
      amount: 0,        // ✅ SAFE DEFAULT
      due_date: null
    };

    await external_axios_namespaceObject["default"].post(
      `${BACKEND_URL}/bill/sync`,
      payload,
      authHeaders()
    );

    synced++;
  }

  console.log(`✅ Bills synced: ${synced}`);
}





/* --------------------------
   5. SYNC SALES ORDERS
--------------------------- */
/* --------------------------
   5. SYNC SALES ORDERS
--------------------------- */


//inventory data 

async function fetchStockSummary() {
  if (!ACTIVE_COMPANY_GUID) return;

  const xml = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Stock Summary</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVFROMDATE>20240401</SVFROMDATE>
     <SVTODATE>20251231</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const res = await external_axios_namespaceObject["default"].post(TALLY_URL, xml, {
    headers: { "Content-Type": "text/xml" },
  });

  console.log("RAW STOCK XML:\n", res.data);

  const parsed = await (0,external_xml2js_namespaceObject.parseStringPromise)(res.data);

  // 🔥 KEY FIX: read flat nodes directly under ENVELOPE
  const names = parsed?.ENVELOPE?.DSPACCNAME || [];
  const infos = parsed?.ENVELOPE?.DSPSTKINFO || [];

  const nameArr = Array.isArray(names) ? names : [names];
  const infoArr = Array.isArray(infos) ? infos : [infos];

  let count = 0;

  for (let i = 0; i < nameArr.length; i++) {
    const itemName = nameArr[i]?.DSPDISPNAME?.[0];
    if (!itemName || itemName === "General") continue;

    const stk = infoArr[i]?.DSPSTKCL?.[0] || {};

    const qty = parseTallyNumber(stk.DSPCLQTY?.[0] || "0");
    const value = parseTallyNumber(stk.DSPCLAMTA?.[0] || "0");

    await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/stock-summary/sync`,
  {
    company_guid: ACTIVE_COMPANY_GUID,
    item_name: itemName.trim(),
    closing_qty: qty,
    closing_value: value,
  },
  authHeaders()
);


    count++;
  }

  console.log(`📊 Stock Summary synced: ${count} items`);
}






function parseTallyNumber(val = "0") {
  return Number(
    val
      .replace(/\(/g, "-")
      .replace(/\)/g, "")
      .replace(/,/g, "")
      .replace(/[^0-9.-]/g, "")
  );
}

 // STOCK MASTER SYNC (Items)

async function syncStockMasters() {
  if (!ACTIVE_COMPANY_GUID) return;

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>StockItem Collection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="StockItem Collection">
      <TYPE>StockItem</TYPE>
      <FETCH>
       NAME,GUID,BASEUNITS,OPENINGBALANCE,OPENINGVALUE
      </FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await external_axios_namespaceObject["default"].post(TALLY_URL, xmlRequest, {
    headers: { "Content-Type": "text/xml" },
  });

  const parsed = await (0,external_xml2js_namespaceObject.parseStringPromise)(res.data);

  const items =
    parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.STOCKITEM || [];

  const itemArray = Array.isArray(items) ? items : [items];

  console.log("📦 Stock Items:", itemArray.length);

  for (const i of itemArray) {
   // await axios.post(`${BACKEND_URL}/stock-item/sync`, {
    //  stock_guid: i.GUID?.[0] || i.GUID?._,
  await external_axios_namespaceObject["default"].post(
  `${BACKEND_URL}/stock-item/sync`,
  {
    item_guid: i.GUID?.[0] || i.GUID?._,
    company_guid: ACTIVE_COMPANY_GUID,
    name: i.NAME?.[0],
    unit: i.BASEUNITS?.[0],
    opening_qty: Number(
      (i.OPENINGBALANCE?.[0] || "0").replace(/[^0-9.-]/g, "")
    ),
    opening_value: parseTallyNumber(i.OPENINGVALUE?.[0]),
  },
  authHeaders()
);

  }
}
/* --------------------------
  Licence Logic
--------------------------- */




/* --------------------------
   START AUTO SYNC
--------------------------- */
let companiesSynced = false;

async function syncAllData() {
  console.log("🔄 Sync started...");

  try {
    const companies = await syncCompanies();

    for (const company of companies) {
      console.log("🔐 Syncing company:", company.name);

      
  // 🔑 THIS LINE IS REQUIRED
  ACTIVE_COMPANY_GUID = company.company_guid;

  // 🔄 Notify backend that sync has STARTED
try {
  await external_axios_namespaceObject["default"].post(
    `${BACKEND_URL}/agent-status/sync-started`,
    { company_guid: company.company_guid },
    authHeaders()
  );
} catch (err) {
  console.error("⚠️ Sync-started update failed:", err.message);
}

      // STEP-2: ONLY ledger sync
      await syncLedgers(company);
         await syncStockMasters();
      await fetchStockSummary();
      await syncVouchers();
try {
  await syncBills();
} catch (err) {
  console.error("⚠️ Bills sync failed (non-blocking):", err.message);
  }

      // ✅ THIS IS THE FIX
      try {
        await external_axios_namespaceObject["default"].post(
          `${BACKEND_URL}/agent-status/sync-status`,
          { company_guid: company.company_guid },
          authHeaders()
        );
      } catch (err) {
        console.error("⚠️ Sync status update failed:", err.message);
      }
    }

    console.log("✅ Sync complete!\n");
  } catch (err) {
    console.error("⛔ Sync failed:", err.message);
  }
}






(async () => {
  console.log("🟢 Tally Agent starting...");

  await agentLogin();     // 🔐 LOGIN FIRST
  await syncAllData();    // 🔄 THEN SYNC

  setInterval(syncAllData, SYNC_INTERVAL);
})();



