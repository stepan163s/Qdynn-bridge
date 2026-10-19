// Shipped bridge JS + real relay HTTP endpoints on loopback. Only the initial
// WebSocket opening is deliberately failed to exercise WS -> HTTPS fallback.
const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const {createServer} = require('node:net');
const {createHmac} = require('node:crypto');
const path = require('node:path');
const vm = require('node:vm');
const http = require('node:http');
const {Readable} = require('node:stream');

const binary = path.resolve(process.argv[2] || 'target/release/qdynn-bridge');
const runtimeFallback = process.argv.includes('--runtime-fallback');
const hostname = 'interop.example';
const secret = '000102030405060708090a0b0c0d0e0f';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let child, context;

// Undici owns the Host header. Use HTTP directly so loopback exercises the
// configured virtual host, preserving streaming bodies and cancellation.
function localFetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    const request = http.request(url, {method: options.method || 'GET',
      headers: options.headers, signal: options.signal}, response => {
      resolve(new Response(response.statusCode === 204 ? null : Readable.toWeb(response),
        {status: response.statusCode, headers: response.headers}));
      if (response.statusCode === 204) response.resume();
    });
    request.on('error', reject);
    request.end(options.body instanceof ArrayBuffer ? Buffer.from(options.body) : options.body);
  });
}

async function waitUntil(predicate, message, milliseconds = 10000) {
  const deadline = Date.now() + milliseconds;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(20);
  }
  throw new Error(message);
}

async function stopServer() {
  const process = child;
  if (!process || process.exitCode !== null) return;
  const exited = new Promise(resolve => process.once('exit', resolve));
  const timer = setTimeout(() => process.kill('SIGKILL'), 3000);
  process.kill('SIGTERM');
  await exited;
  clearTimeout(timer);
}

(async () => {
  const listener = createServer();
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const base = 'http://127.0.0.1:' + port;
  let childLog = '';
  async function startServer() {
    child = spawn(binary, ['--hostname', hostname, '--secret', secret,
      '--listen', '127.0.0.1:' + port, '--carrier-mode', 'auto'],
      {stdio: ['ignore', 'pipe', 'pipe']});
    child.stdout.on('data', data => {childLog = (childLog + data).slice(-4096)});
    child.stderr.on('data', data => {childLog = (childLog + data).slice(-4096)});
    let error;
    child.on('error', value => {error = value});
    await waitUntil(async () => {
      if (error) throw error;
      if (child.exitCode !== null) throw new Error('relay exited: ' + childLog);
      try { return (await fetch(base, {headers: {Host: hostname}})).ok; }
      catch { return false; }
    }, 'relay startup timeout');
  }
  await startServer();
  const capability = createHmac('sha256', Buffer.from(secret, 'hex'))
    .update('tdesktop-web-proxy-bridge-v1\n' + hostname).digest('base64url');
  const page = await (await localFetch(base + '/?bridge=' + capability,
    {headers: {Host: hostname}})).text();
  const source = page.slice(page.indexOf('(()=>{'), page.indexOf('</script>', page.indexOf('(()=>{')));
  assert(source.startsWith('(()=>{'), 'rendered bridge script missing');
  const output = [], tokens = [], modes = [], sockets = [];
  let recoveries = 0;
  class FailedWebSocket {
    static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
    constructor() {
      this.readyState = 0; this.bufferedAmount = 0; sockets.push(this);
      queueMicrotask(() => {
        if(runtimeFallback&&sockets.length===1){this.readyState=1;this.onopen?.()}
        else this.onerror?.();
      });
    }
    close() { if (this.readyState === 3) return; this.readyState = 3; this.onclose?.(); }
  }
  context = vm.createContext({
    AbortController, ArrayBuffer, Uint8Array, DataView, URL, TextDecoder,
    setTimeout, clearTimeout, queueMicrotask, console,
    WebSocket: FailedWebSocket, addEventListener() {}, parent: {},
    location: {hash: '#android=' + 'a'.repeat(43), pathname: '/'},
    history: {replaceState() {}},
    TelegramWebProxy: {postMessage(value) {output.push(value)}},
    async fetch(url, options) {
      const route = new URL(url).pathname;
      if (route === '/api/v1/recover') recoveries++;
      const response = await localFetch(base + route, {...options,
        headers: {...options.headers, Host: hostname, Origin: 'https://' + hostname}});
      if (route === '/api/v1/session' && options.method === 'POST') {
        assert.equal(response.status, 200, 'real server must accept bridge HELLO');
        tokens.push(response.headers.get('x-session-token'));
        modes.push(response.headers.get('x-carrier-mode'));
      }
      return response;
    },
  });
  vm.runInContext(source, context);
  context.TelegramWebProxy.onmessage({data: Uint8Array.from([16,0,0,0,0,0,0,1,1]).buffer});
  const welcomes = () => output.filter(value => value instanceof ArrayBuffer && new Uint8Array(value)[0] === 17);
  await waitUntil(() => welcomes().length === 1, 'first fallback did not deliver WELCOME');
  if(runtimeFallback){
    assert.deepEqual(modes,['websocket']);
    sockets[0].close();
    await waitUntil(()=>tokens.length===3,'runtime WS rejection did not fall back to HTTPS');
  }
  const expectedModes=runtimeFallback?['websocket','websocket','https']:['websocket','https'];
  assert.deepEqual(modes, expectedModes);
  assert.equal(recoveries, runtimeFallback?2:1);
  const old = tokens.at(-1);
  await stopServer();
  await startServer();
  await waitUntil(() => tokens.length === expectedModes.length+1, 'bridge did not recover after real process restart', 20000);
  assert.notEqual(tokens.at(-1), old);
  assert.deepEqual(modes, [...expectedModes,'https']);
  assert.equal(recoveries, runtimeFallback?3:2);
  await sleep(50);
  assert.equal(welcomes().length, 1, 'runtime recovery duplicated WELCOME');
  console.log('PASS: real HTTP bridge '+(runtimeFallback?'runtime':'startup')+' fallback, valid HELLO, one WELCOME, process restart with stale bearer');
})().catch(error => {console.error(error); process.exitCode = 1})
  .finally(async () => {
    context?.TelegramWebProxy.onmessage?.({data: {t: 'close'}});
    await stopServer();
  });
