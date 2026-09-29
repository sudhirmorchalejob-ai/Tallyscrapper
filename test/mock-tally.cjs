// test/mock-tally.cjs -> node test/mock-tally.cjs (then run agent with TALLY_PORT=9100)
const http = require('http');
const PORT = process.env.MOCK_TALLY_PORT ? Number(process.env.MOCK_TALLY_PORT) : 9100;
const POISON = '20230715';
const DOWN_MS = process.env.MOCK_DOWN_MS ? Number(process.env.MOCK_DOWN_MS) : 15000;
let server;

function handler(req, res) {
  let b = '';
  req.on('data', (c) => (b += c));
  req.on('end', () => {
    const from = /SVFROMDATE[^>]*>(\d{8})/.exec(b)?.[1];
    const to = /SVTODATE[^>]*>(\d{8})/.exec(b)?.[1];
    const d = (s) => new Date(`${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`);
    const days = from && to ? Math.round((d(to) - d(from)) / 86400000) + 1 : 1;
    const hitsPoison = from && to && from <= POISON && POISON <= to;
    const filtered = /MasterID\s*=/.test(b);

    const isPoisonMaster = /MasterID\s*=\s*2023071599\b/.test(b);

    if (days > 3 || (hitsPoison && !filtered && /VchDetail/.test(b)) || isPoisonMaster) {
      console.log(`[mock] CRASH on ${from}..${to} (${days}d, poisonMaster=${isPoisonMaster})`);
      req.socket.destroy();
      try { server.close(); } catch {}
      try { if (server.closeAllConnections) server.closeAllConnections(); } catch {}
      return setTimeout(start, DOWN_MS);
    }

    if (/CompList/.test(b)) {
      return res.end('<ENVELOPE><NAME>TEST CO</NAME></ENVELOPE>');
    }

    if (/VchHeaders/.test(b)) {
      if (hitsPoison) {
        return res.end(
          `<ENVELOPE>
            <VOUCHER><GUID>g-20230715-good</GUID><MASTERID>2023071501</MASTERID><ALTERID>1</ALTERID><DATE>20230715</DATE><VOUCHERTYPENAME>Sales</VOUCHERTYPENAME><ISCANCELLED>No</ISCANCELLED><ISOPTIONAL>No</ISOPTIONAL></VOUCHER>
            <VOUCHER><GUID>g-20230715-poison</GUID><MASTERID>2023071599</MASTERID><ALTERID>1</ALTERID><DATE>20230715</DATE><VOUCHERTYPENAME>Sales</VOUCHERTYPENAME><ISCANCELLED>No</ISCANCELLED><ISOPTIONAL>No</ISOPTIONAL></VOUCHER>
          </ENVELOPE>`
        );
      }
      return res.end(
        `<ENVELOPE><VOUCHER><GUID>g-${from}</GUID><MASTERID>${from}</MASTERID><ALTERID>1</ALTERID><DATE>${from}</DATE><VOUCHERTYPENAME>Sales</VOUCHERTYPENAME><ISCANCELLED>No</ISCANCELLED><ISOPTIONAL>No</ISOPTIONAL></VOUCHER></ENVELOPE>`
      );
    }

    const mIdMatch = /MasterID\s*=\s*(\d+)/.exec(b);
    const mId = mIdMatch ? mIdMatch[1] : from;
    res.end(
      `<ENVELOPE><VOUCHER><GUID>g-${mId}</GUID><MASTERID>${mId}</MASTERID><ALTERID>1</ALTERID><DATE>${from || '20230715'}</DATE><VOUCHERTYPENAME>Sales</VOUCHERTYPENAME><VOUCHERNUMBER>V-${mId}</VOUCHERNUMBER><ALLLEDGERENTRIES.LIST><LEDGERNAME>Cash</LEDGERNAME><AMOUNT>-100</AMOUNT><ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE></ALLLEDGERENTRIES.LIST></VOUCHER></ENVELOPE>`
    );
  });
}

function start() {
  server = http.createServer(handler).listen(PORT, () => {
    console.log('[mock] up on', PORT);
  });
}

if (require.main === module) {
  start();
}

module.exports = { start, stop: () => server && server.close() };
