// Synthetic startup lifecycle check; no network, real browser, or real credentials.
// Optional argument: upstream internal/bridge/page.go for a reference run.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const filename = process.argv[2] || path.resolve(__dirname, '../crates/qdynn-server/src/bridge.js');
const source = fs.readFileSync(filename, 'utf8');
const start = source.indexOf('(()=>{');
let end = source.indexOf('</script>', start);
if (end < 0) end = source.lastIndexOf('})();') + 4;
assert(start >= 0 && end > start, 'bridge script not found');
let script = source.slice(start, end);
for (const [key, value] of Object.entries({
  __ORIGIN__: '"https://audit.example"',
  __BRIDGE_CAPABILITY__: '"synthetic-capability"',
  __RELAY_BASE__: '"https://audit.example/"',
  __BOOTSTRAP__: '"synthetic-bootstrap"',
  __CARRIER_MODE__: '"websocket-lanes"',
  __BATCH_LIMIT__: '2097152',
  __STARTUP_TIMEOUT_MS__: '15000',
  __WEBSOCKET_OPEN_TIMEOUT_MS__: '4000',
  __REQUEST_TIMEOUT_MS__: '90000',
})) script = script.replaceAll(key, value);

const context = vm.createContext({ assert, AbortController, URL, Response, ReadableStream, setTimeout, clearTimeout, queueMicrotask });
vm.runInContext(`
  const sockets = [], statuses = [];
  const location = { hash: '#android=' + 'a'.repeat(43), pathname: '/' };
  const history = { replaceState() {} };
  const addEventListener = () => {};
  const TelegramWebProxy = globalThis.TelegramWebProxy = {
    postMessage(data) { if (typeof data === 'string') statuses.push(JSON.parse(data)); }
  };
  function frame(type, id, payload = []) {
    const bytes = new Uint8Array(8 + payload.length);
    bytes.set([type, id >>> 16, (id >>> 8) & 255, id & 255]);
    new DataView(bytes.buffer).setUint32(4, payload.length);
    bytes.set(payload, 8);
    return bytes.buffer;
  }
  const fetch = async () => new Response(frame(17, 0), {
    status: 200,
    headers: {'X-Carrier-Mode':'websocket-lanes', 'X-Session-Token':'synthetic-session', 'X-Down-Cursor':'0'}
  });
  class WebSocket {
    static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
    constructor() { this.readyState = 0; this.bufferedAmount = 0; sockets.push(this); }
    send() { assert.equal(this.readyState, WebSocket.OPEN); }
    close() { this.readyState = WebSocket.CLOSED; this.onclose?.(); }
  }
  function send(type, id, payload) { TelegramWebProxy.onmessage({data:frame(type,id,payload)}); }
`, context);
vm.runInContext(script, context);
vm.runInContext(`
  (async () => {
    send(16, 0, [1]);
    for (let i = 0; i < 20; i++) await Promise.resolve();
    assert(statuses.some(s => s.state === 'connected'), 'bootstrap did not finish');
    send(1, 1);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].readyState, WebSocket.CONNECTING);
    send(3, 1);
    assert.equal(sockets[0].readyState, WebSocket.CLOSED,
      'CLOSE during initial handshake must cancel the pending WebSocket');
    assert(!statuses.some(s => s.state === 'failed'), 'cancellation killed the session');
    sockets[0].readyState = WebSocket.OPEN;
    sockets[0].onopen?.();
    assert.equal(sockets[0].readyState, WebSocket.CLOSED, 'late onopen resurrected cancelled lane');
    for (let id = 2; id <= 1502; id++) {
      send(1, id); send(2, id, new Uint8Array(65536)); send(3, id);
    }
    assert(!statuses.some(s => s.state === 'failed'), 'cancelled lanes retained queue reservations');

    TelegramWebProxy.onmessage({data:{t:'close'}});
  })()
`, context).then(() => {
  console.log('PASS: pending lane handshake cancelled without failing the session');
}).catch(error => {
  console.error(error);
  process.exitCode = 1;
});
