// Exercises bridge startup deadlines, streamed body limits, cancellation and late WS events.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const source = fs.readFileSync(path.resolve(__dirname,
  '../crates/qdynn-server/src/bridge.js'), 'utf8');

function frame(type, id, payload = []) {
  const bytes = new Uint8Array(8 + payload.length);
  bytes.set([type, id >>> 16, (id >>> 8) & 255, id & 255]);
  new DataView(bytes.buffer).setUint32(4, payload.length);
  bytes.set(payload, 8);
  return bytes.buffer;
}

async function setup(mode, fetchMode, failures = 0, allowRecovery = false) {
  let bridge, clockOffset=0, progressTick=null, progressTimer=null;
  const schedule=(fn,ms)=>{
    const timer=setTimeout(fn,ms);
    if(ms===1000){progressTick=fn;progressTimer=timer}
    return timer;
  };
  const listeners=new Map();
  const document={visibilityState:'visible',addEventListener(name,fn){listeners.set(name,fn)}};
  class Clock extends Date {static now(){return Date.now()+clockOffset}}

  const output = [], sockets = [], calls=[];
  const errors=[];
  const testConsole={error(...values){errors.push(values)},log(){},warn(){}};
  const reports = [];
  let sessionRequests = 0;
  let sawInitialFailure = false;
  let recoverRequests = 0, recoveryAborted=false;
  let rejectDown = null;
  class WebSocket {
    static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
    constructor() {
      this.readyState = 0; this.bufferedAmount = 0; sockets.push(this);
      if(fetchMode==='ws-reopen-refused'&&sockets.length>1)queueMicrotask(()=>this.onerror?.());
      if (fetchMode === 'ws-runtime-recovery' && sockets.length > 1) queueMicrotask(() => {
        if (this.readyState !== WebSocket.CLOSED) { this.readyState = WebSocket.OPEN; this.onopen?.(); }
      });
    }
    send() { assert.equal(this.readyState, WebSocket.OPEN); }
    close() { if (this.readyState === WebSocket.CLOSED) return; this.readyState = WebSocket.CLOSED; this.onclose?.(); }
  }
  const context = vm.createContext({
    assert, AbortController, ArrayBuffer, Uint8Array, DataView, URL, Response, ReadableStream, TextDecoder, WebSocket, console:testConsole,
    setTimeout:schedule, clearTimeout, queueMicrotask, Date:Clock, Math, document,
  });
  context.TelegramWebProxy = bridge = {
    onmessage: null,
    postMessage(value) { output.push(value); },
  };
  context.location = {hash: '#android=' + 'a'.repeat(43), pathname: '/'};
  context.history = {replaceState() {}};
  context.globalThis = context;
  context.addEventListener = (name,fn) => listeners.set(name,fn);
  context.parent = {};
  context.fetch = async (_url, options) => {
    calls.push({url:_url,method:options.method,authorization:options.headers?.Authorization,
      carrierMode:options.headers?.['X-Carrier-Mode']});
    if (_url.endsWith('/api/v1/recover')) {
      recoverRequests++;
      if (!allowRecovery) return new Response(null, {status: 503});
      if(fetchMode==='recover-pending')return new Promise((resolve,reject)=>{options.signal.addEventListener('abort',()=>{recoveryAborted=true;reject(new Error('cancelled'))},{once:true})});
      if (fetchMode === 'oversized-recovery') return new Response(new Uint8Array(1025), {status: 200});
      if (fetchMode === 'slow-recovery') await sleep(50);
      return new Response(JSON.stringify({bootstrap: 'fresh-recovery-bootstrap-token-0123456789'}), {status: 200,
        headers: {'Content-Type': 'application/json'}});
    }
    if (_url.endsWith('/api/v1/report')) {
      reports.push(JSON.parse(options.body));
      return new Response(null, {status: 204});
    }
    if (_url.endsWith('/api/v1/down')) {
      return new Promise((resolve, reject) => {
        if (fetchMode === 'runtime-down-failure') rejectDown = () => resolve(new Response(null, {status: 410}));
        options.signal.addEventListener('abort', () => reject(new Error('downlink cancelled')), {once: true});
      });
    }
    if (_url.endsWith('/api/v1/up')) {
      return new Response(null, {status: 204, headers: {'X-Up-Ack': options.headers['X-Up-Seq']}});
    }
    if (_url.endsWith('/api/v1/session') && options.method === 'POST') {
      sessionRequests++;
      assert.deepEqual(Array.from(new Uint8Array(options.body)), [16,0,0,0,0,0,0,1,1], 'every session HELLO must include version byte');
    }
    if (_url.endsWith('/api/v1/session') && options.method === 'POST' &&
        options.headers.Authorization === 'Bearer synthetic-bootstrap' && failures > 0 && !sawInitialFailure) {
      sawInitialFailure=true;
      return new Response(null, {status: 500});
    }
    if(_url.endsWith('/api/v1/session')&&options.method==='POST'&&fetchMode.startsWith('retry-')&&sessionRequests===1)return new Response(null,{status:Number(fetchMode.slice(6))});
    if (_url.endsWith('/api/v1/session') && options.method === 'DELETE') return new Response(null, {status: 204});
    if (fetchMode === 'stalled-body') {
      const body = new ReadableStream({start(controller) {
        options.signal.addEventListener('abort', () => controller.error(new Error('request aborted')), {once: true});
      }});
      return new Response(body, {status: 200, headers: {
        'X-Carrier-Mode': mode, 'X-Session-Token': 'synthetic-session', 'X-Down-Cursor': '0',
      }});
    }
    if (fetchMode === 'oversized-body') {
      return new Response(new Uint8Array(9), {status: 200, headers: {
        'X-Carrier-Mode': mode, 'X-Session-Token': 'synthetic-session', 'X-Down-Cursor': '0',
      }});
    }
    return new Response(frame(17, 0), {status: 200, headers: {
      'X-Carrier-Mode': options.headers?.['X-Carrier-Mode'] || mode,
      'X-Session-Token': 'synthetic-session', 'X-Down-Cursor': '0',
    }});
  };

  let script = source
    .replace('__STARTUP_TIMEOUT_MS__', fetchMode.startsWith('retry-')?'1000':'80')
    .replace('__WEBSOCKET_OPEN_TIMEOUT_MS__', '40')
    .replace('__REQUEST_TIMEOUT_MS__', '90000')
    .replace('__ORIGIN__', '"https://audit.example"')
    .replace('__BRIDGE_CAPABILITY__', '"synthetic-capability"')
    .replace('__BOOTSTRAP__', '"synthetic-bootstrap"')
    .replace('__CARRIER_MODE__', JSON.stringify(mode))
    .replace('__BATCH_LIMIT__', '2097152');
  vm.runInContext(script, context);
  return {bridge, output, sockets, reports, calls, errors, document,
    tickProgress:()=>{clearTimeout(progressTimer);const tick=progressTick;progressTick=null;tick?.()},
    event: name => listeners.get(name)?.(), advanceClock: ms => {clockOffset+=ms}, sessionRequests: () => sessionRequests,
    recoveryAborted:()=>recoveryAborted, recoverRequests: () => recoverRequests, failDown: () => rejectDown?.()};
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
  const stalled = await setup('websocket', 'stalled-body');
  stalled.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(140);
  assert(stalled.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'a response body stall must end within the shared startup deadline');
  await sleep(0);
  assert.deepEqual(stalled.reports[0], {phase: 'startup', reason: 'timeout'});

  const oversized = await setup('websocket', 'oversized-body');
  oversized.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(30);
  assert(oversized.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'a WELCOME body with an invalid size must be rejected');
  assert.equal(oversized.sessionRequests(), 1, 'a malformed protocol response must not be retried');
  await sleep(0);
  assert(oversized.reports.length === 1 && oversized.reports[0].phase === 'startup',
    'a failed startup must emit one bounded reason-code report');

  const ws = await setup('websocket', 'normal');
  ws.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(5);
  assert.equal(ws.sockets.length, 1);
  await sleep(100);
  assert.equal(ws.sockets[0].readyState, 3, 'stalled multiplexed upgrade must be closed');
  assert(ws.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'a stalled initial upgrade must fail within its deadline');
  ws.sockets[0].readyState = 1;
  ws.sockets[0].onopen?.();
  assert.equal(ws.sockets[0].readyState, 3, 'late onopen must not resurrect the failed attempt');

  const autoCarrier = await setup('auto', 'normal', 0, true);
  autoCarrier.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(150);
  assert.equal(autoCarrier.recoverRequests(), 1, 'failed initial WebSocket must request one fresh incarnation');
  assert.equal(autoCarrier.sessionRequests(), 2, 'carrier fallback must create exactly one replacement session; calls=' + JSON.stringify(autoCarrier.calls) + ' reports=' + JSON.stringify(autoCarrier.reports) + ' errors=' + JSON.stringify(autoCarrier.errors) + ' output=' + JSON.stringify(autoCarrier.output));
  assert.deepEqual(autoCarrier.calls.filter(call => call.url.endsWith('/api/v1/session') && call.method === 'POST')
    .map(call => call.carrierMode), ['websocket', 'https']);
  assert(autoCarrier.output.some(value => typeof value === 'string' && JSON.parse(value).state === 'connected'),
    'HTTPS fallback should connect after WebSocket opening fails');
  assert.equal(autoCarrier.output.filter(value => value instanceof ArrayBuffer && new Uint8Array(value)[0]===17).length, 1,
    'first fallback must deliver exactly one WELCOME');
  autoCarrier.bridge.onmessage({data: {t: 'close'}});

  const expiredFallback = await setup('auto', 'slow-recovery', 0, true);
  expiredFallback.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(130);
  assert.equal(expiredFallback.recoverRequests(), 1);
  assert.equal(expiredFallback.sessionRequests(), 1,
    'a recovery response arriving after the original startup deadline must not create a replacement session');
  assert(expiredFallback.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'carrier fallback must fail when its shared startup budget is exhausted');

  const autoRuntimeRecovery = await setup('auto', 'ws-runtime-recovery', 0, true);
  autoRuntimeRecovery.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(5);
  autoRuntimeRecovery.sockets[0].readyState = 1;
  autoRuntimeRecovery.sockets[0].onopen?.();
  for (let i = 0; i < 20 && !autoRuntimeRecovery.output.some(value =>
      typeof value === 'string' && JSON.parse(value).state === 'connected'); i++) await sleep(1);
  autoRuntimeRecovery.bridge.onmessage({data: frame(1, 12)});
  autoRuntimeRecovery.sockets[0].readyState = 3;
  autoRuntimeRecovery.sockets[0].onclose?.();
  await sleep(40);
  assert.deepEqual(autoRuntimeRecovery.calls.filter(call => call.url.endsWith('/api/v1/session') && call.method === 'POST')
    .map(call => call.carrierMode), ['websocket', 'websocket'],
    'runtime recovery after commit must keep the selected carrier');
  assert.equal(autoRuntimeRecovery.recoverRequests(), 1);
  autoRuntimeRecovery.bridge.onmessage({data: {t: 'close'}});

  const lane = await setup('websocket-lanes', 'normal');
  lane.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(5);
  lane.bridge.onmessage({data: frame(1, 7)});
  assert.equal(lane.sockets.length, 1);
  await sleep(80);
  assert.equal(lane.sockets[0].readyState, 3, 'stalled lane upgrade must be closed');
  assert(!lane.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'one stalled lane must not fail its sibling session');
  lane.sockets[0].readyState = 1;
  lane.sockets[0].onopen?.();
  assert.equal(lane.sockets[0].readyState, 3, 'late lane onopen must not resurrect a retired lane');
  lane.bridge.onmessage({data:{t:'close'}});

  const recovery = await setup('https', 'normal', 1, true);
  recovery.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(450);
  assert.equal(recovery.recoverRequests(), 1, 'failure must have exactly one recovery owner');
  assert.equal(recovery.sessionRequests(), 2, 'recovery should create one replacement session; calls=' + JSON.stringify(recovery.calls) + ' output=' + JSON.stringify(recovery.output));
  assert(recovery.output.some(value => typeof value === 'string' && JSON.parse(value).state === 'connected'),
    'replacement session should reattach to the existing Android bridge');
  assert.equal(recovery.output.filter(value => value instanceof ArrayBuffer && new Uint8Array(value)[0]===17).length, 1,
    'startup recovery must deliver the first WELCOME to native');
  recovery.bridge.onmessage({data: {t: 'close'}});

  const wsRecovery = await setup('websocket', 'ws-runtime-recovery', 0, true);
  wsRecovery.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(5);
  assert.equal(wsRecovery.sockets.length, 1);
  wsRecovery.sockets[0].readyState = 1;
  wsRecovery.sockets[0].onopen?.();
  wsRecovery.bridge.onmessage({data: frame(1, 9)});
  wsRecovery.sockets[0].readyState = 3;
  wsRecovery.sockets[0].onclose?.();
  await sleep(40);
  assert.equal(wsRecovery.recoverRequests(), 1, 'multiplexed WebSocket loss must start one recovery');
  assert.equal(wsRecovery.sessionRequests(), 2, 'multiplexed WebSocket loss must create one replacement session');
  assert.equal(wsRecovery.sockets.length, 2, 'recovery must re-open the configured WebSocket carrier');
  assert.equal(wsRecovery.output.filter(value => value instanceof ArrayBuffer && new Uint8Array(value)[0]===17).length, 1,
    'WebSocket recovery must not send a second WELCOME');
  assert(wsRecovery.output.some(value => value instanceof ArrayBuffer &&
      new DataView(value).getUint8(0) === 3 && new DataView(value).getUint8(3) === 9),
    'WebSocket recovery must close the old native stream');
  wsRecovery.bridge.onmessage({data: {t: 'close'}});

  const recoveryBurst = await setup('https', 'normal', 1, true);
  recoveryBurst.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(0);
  recoveryBurst.bridge.onmessage({data: frame(1, 4)});
  await sleep(450);
  assert.equal(recoveryBurst.recoverRequests(), 1, 'overlapping startup failures must share one recovery request');
  assert.equal(recoveryBurst.sessionRequests(), 2, 'overlapping startup failures must create one replacement session');
  assert.equal(recoveryBurst.reports.length, 1, 'overlapping startup failures must produce one diagnostic report');
  recoveryBurst.bridge.onmessage({data: {t: 'close'}});

  const oversizedRecovery = await setup('https', 'oversized-recovery', 1, true);
  oversizedRecovery.bridge.onmessage({data: frame(16, 0, [1])});
  await sleep(100);
  assert.equal(oversizedRecovery.sessionRequests(), 1, 'oversized recovery responses must be rejected before session creation');
  assert(oversizedRecovery.output.includes(JSON.stringify({t: 'status', state: 'failed'})),
    'oversized recovery response must fail closed');

  const runtimeRecovery = await setup('https', 'runtime-down-failure', 0, true);
  runtimeRecovery.bridge.onmessage({data: frame(16, 0, [1])});
  for (let i = 0; i < 50 && !runtimeRecovery.output.some(value =>
      typeof value === 'string' && JSON.parse(value).state === 'connected'); i++) await sleep(2);
  runtimeRecovery.bridge.onmessage({data: frame(1, 7)});
  await sleep(5);
  runtimeRecovery.failDown();
  await sleep(30);
  assert.equal(runtimeRecovery.sessionRequests(), 2, 'runtime failure should create one replacement session; calls=' + JSON.stringify(runtimeRecovery.calls) + ' output=' + JSON.stringify(runtimeRecovery.output));
  const closedStream = runtimeRecovery.output.some(value => value instanceof ArrayBuffer &&
      new DataView(value).getUint8(0) === 3 &&
      ((new DataView(value).getUint8(1) << 16) | (new DataView(value).getUint8(2) << 8) | new DataView(value).getUint8(3)) === 7);
  assert(closedStream, 'recovery must explicitly close the old Android stream so Telegram can reconnect it');
  runtimeRecovery.bridge.onmessage({data: {t: 'close'}});
  for(const code of [408,429,502,503,504]){
    const retry=await setup('https','retry-'+code);
    retry.bridge.onmessage({data:frame(16,0,[1])});await sleep(330);
    assert.equal(retry.sessionRequests(),2,'transient status '+code+' must retry the same HELLO');
    assert.equal(retry.recoverRequests(),0);
    assert.equal(retry.output.filter(v=>v instanceof ArrayBuffer&&new Uint8Array(v)[0]===17).length,1);
    retry.bridge.onmessage({data:{t:'close'}});
  }

  const teardown=await setup('https','recover-pending',1,true);
  teardown.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  assert.equal(teardown.recoverRequests(),1);
  teardown.bridge.onmessage({data:{t:'close'}});await sleep(5);
  assert(teardown.recoveryAborted(),'page teardown must abort the recovery fetch');
  assert.equal(teardown.sessionRequests(),1,'cancelled recovery must not create a session');

  const resume = await setup('websocket', 'ws-runtime-recovery', 0, true);
  resume.bridge.onmessage({data:frame(16,0,[1])});
  await sleep(5);resume.sockets[0].readyState=1;resume.sockets[0].onopen();await sleep(5);
  resume.advanceClock(100000);resume.tickProgress();
  assert.equal(resume.recoverRequests(),0,'healthy idle socket must survive silence');
  resume.event('online');await sleep(5);
  assert.equal(resume.recoverRequests(),0,'online without an interruption must not reset a healthy session');
  resume.event('offline');resume.event('online');resume.event('online');await sleep(30);
  assert.equal(resume.recoverRequests(),1,'network resume must have a single recovery owner');
  const beforeLate=resume.output.length;
  resume.sockets[0].onmessage({data:frame(2,99,[7])});resume.sockets[0].onclose();
  assert.equal(resume.output.length,beforeLate,'old WebSocket callbacks must be inert');
  assert.equal(resume.output.filter(v=>v instanceof ArrayBuffer&&new Uint8Array(v)[0]===17).length,1);
  resume.bridge.onmessage({data:{t:'close'}});

  const foreground = await setup('https','normal',0,true);
  foreground.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  foreground.document.visibilityState='hidden';foreground.event('visibilitychange');
  foreground.advanceClock(100);foreground.document.visibilityState='visible';foreground.event('visibilitychange');
  assert.equal(foreground.recoverRequests(),0,'brief backgrounding must not reset the session');
  foreground.document.visibilityState='hidden';foreground.event('visibilitychange');foreground.advanceClock(90001);
  foreground.document.visibilityState='visible';foreground.event('visibilitychange');await sleep(30);
  assert.equal(foreground.recoverRequests(),1,'long background pause must recover on return');
  const beforeLateUp=foreground.calls.filter(c=>c.url.endsWith('/api/v1/up')).length;
  foreground.bridge.onmessage({data:frame(2,42,[7])});await sleep(5);
  assert.equal(foreground.calls.filter(c=>c.url.endsWith('/api/v1/up')).length,beforeLateUp,'retired native DATA must not enter the new session');
  foreground.bridge.onmessage({data:{t:'close'}});
  foreground.event('offline');foreground.event('online');await sleep(5);
  assert.equal(foreground.recoverRequests(),1,'closed bridge must not be resurrected');

  const blockedSend=await setup('websocket','ws-runtime-recovery',0,true);
  blockedSend.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  blockedSend.sockets[0].readyState=1;blockedSend.sockets[0].onopen();await sleep(5);
  blockedSend.sockets[0].bufferedAmount=64;blockedSend.tickProgress();
  blockedSend.advanceClock(89000);blockedSend.tickProgress();
  assert.equal(blockedSend.recoverRequests(),0,'send stall must respect its deadline');
  blockedSend.sockets[0].bufferedAmount=32;blockedSend.tickProgress();
  blockedSend.advanceClock(89000);blockedSend.tickProgress();
  assert.equal(blockedSend.recoverRequests(),0,'draining bytes must reset the progress deadline');
  blockedSend.advanceClock(1001);blockedSend.tickProgress();await sleep(30);
  assert.equal(blockedSend.recoverRequests(),1,'stalled browser send buffer must trigger recovery');
  blockedSend.bridge.onmessage({data:{t:'close'}});

  const switched=await setup('auto','ws-reopen-refused',0,true);
  switched.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  switched.sockets[0].readyState=1;switched.sockets[0].onopen();await sleep(5);
  switched.bridge.onmessage({data:frame(1,5)});
  switched.sockets[0].close();await sleep(25);
  assert.deepEqual(switched.calls.filter(c=>c.url.endsWith('/api/v1/session')&&c.method==='POST').map(c=>c.carrierMode),['websocket','websocket','https']);
  assert.equal(switched.recoverRequests(),2,'one WS recovery and one HTTPS fallback');
  assert.equal(switched.output.filter(v=>v instanceof ArrayBuffer&&new Uint8Array(v)[0]===17).length,1);
  assert.equal(switched.output.filter(v=>v instanceof ArrayBuffer&&new Uint8Array(v)[0]===3).length,1);
  switched.sockets[1].onclose?.();await sleep(0);
  assert.equal(switched.recoverRequests(),2,'late failed socket must not start another fallback');
  switched.bridge.onmessage({data:{t:'close'}});

  const fixed=await setup('websocket','ws-reopen-refused',0,true);
  fixed.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  fixed.sockets[0].readyState=1;fixed.sockets[0].onopen();await sleep(5);
  fixed.sockets[0].close();await sleep(25);
  assert.equal(fixed.sessionRequests(),2,'fixed carrier must not fall back');
  assert(fixed.output.includes(JSON.stringify({t:'status',state:'failed'})));

  const expired=await setup('auto','normal',0,true);
  expired.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  expired.sockets[0].readyState=1;expired.sockets[0].onopen();await sleep(5);
  expired.sockets[0].close();await sleep(5);
  expired.advanceClock(1000);expired.sockets[1].onerror();await sleep(5);
  assert.equal(expired.recoverRequests(),1,'fallback must not receive a new deadline after budget exhaustion');
  assert(expired.output.includes(JSON.stringify({t:'status',state:'failed'})));

  const bulk=await setup('websocket','normal',0,true);
  bulk.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  const bulkSocket=bulk.sockets[0];bulkSocket.readyState=1;bulkSocket.onopen();await sleep(5);
  bulkSocket.send=function(data){this.bufferedAmount+=data.byteLength};
  bulk.bridge.onmessage({data:frame(1,7)});bulkSocket.bufferedAmount=100;bulk.tickProgress();
  for(let i=0;i<4;i++){
    bulk.advanceClock(30000);bulkSocket.bufferedAmount=91;
    bulk.bridge.onmessage({data:frame(2,7,[1])});bulk.tickProgress();
    assert.equal(bulkSocket.bufferedAmount,100);
  }
  await sleep(0);
  assert.equal(bulk.recoverRequests(),0,'drain/refill between timer ticks must count as progress');
  bulk.bridge.onmessage({data:{t:'close'}});

  const lanes=await setup('websocket-lanes','normal',0,true);
  lanes.bridge.onmessage({data:frame(16,0,[1])});await sleep(5);
  lanes.bridge.onmessage({data:frame(1,10)});lanes.bridge.onmessage({data:frame(1,11)});
  for(const socket of lanes.sockets){socket.readyState=1;socket.onopen();socket.bufferedAmount=100}
  lanes.tickProgress();
  for(let i=0;i<4;i++){
    lanes.advanceClock(25000);lanes.sockets[1].bufferedAmount-=10;
    lanes.sockets[1].onmessage({data:frame(2,11,[1])});lanes.tickProgress();
  }
  await sleep(15);
  assert.equal(lanes.recoverRequests(),1,'a healthy lane must not hide a stalled neighbour');
  lanes.bridge.onmessage({data:{t:'close'}});
  console.log('PASS: startup deadlines, lane isolation and single-owner recovery without duplicate WELCOME');
})().catch(error => { console.error(error); process.exit(1); });
