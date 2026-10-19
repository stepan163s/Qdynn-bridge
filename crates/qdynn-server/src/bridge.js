// Portions adapted from telegramdesktop/tproxy-server internal/bridge/page.go.
// See THIRD_PARTY_NOTICES.md for provenance and upstream licensing status.
(()=>{
'use strict';
const relayOrigin=__ORIGIN__,bridgeCapability=__BRIDGE_CAPABILITY__,bootstrap=__BOOTSTRAP__,configuredCarrierMode=__CARRIER_MODE__;
const carrierModes=configuredCarrierMode==='auto'?['websocket','https']:[configuredCarrierMode];
let carrierModeIndex=0,carrierMode=carrierModes[0];
const fragment=location.hash,androidNonce=/^#android=([A-Za-z0-9_-]{43})$/.exec(fragment)?.[1]||'';
history.replaceState(null,'',location.pathname);
let initialized=false,closed=false,port=null,sessionToken='',createStarted=false,recoveryEpoch=0,recoveryPromise=null;
let startupActive=false,startupDeadline=0,nativeWelcomeSent=false;
const startupBudgetMs=__STARTUP_TIMEOUT_MS__,webSocketOpenTimeoutMs=__WEBSOCKET_OPEN_TIMEOUT_MS__,requestTimeoutMs=__REQUEST_TIMEOUT_MS__;
let lifecycleController=new AbortController();
let queuedBytes=0,queuedItems=0,pollController=null,webSocket=null,webSocketTimer=0;
const pending=[],upPending=[],lanes=new Map(),closedLanes=new Set(),closedLaneOrder=[];
const activeNativeStreams=new Set();
const queueLimit=33554432,queueItemLimit=16384,closedLaneLimit=4096;
const laneQueueLimit=8388608,laneItemLimit=1024,batchLimit=__BATCH_LIMIT__;
let upSequence=1,downCursor='0',upRunning=false;
let hiddenSince=null,wasOffline=false,progressTimer=0,socketProgress=new WeakMap();
function stopProgressWatch(){if(progressTimer)clearTimeout(progressTimer);progressTimer=0;socketProgress=new WeakMap()}
function sendProgress(socket,sent=0){
 const buffered=socket.bufferedAmount,now=Date.now();
 let progress=socketProgress.get(socket);
 if(!progress){progress={buffered:0,since:now};socketProgress.set(socket,progress)}
 // Account for newly queued bytes: sustained traffic may drain and refill to
 // the same buffer size between timer ticks. Track each lane independently.
 if(!buffered||!progress.buffered||buffered<progress.buffered+sent)progress.since=now;
 progress.buffered=buffered;
 return buffered>0&&now-progress.since>=requestTimeoutMs;
}
function watchProgress(){
 stopProgressWatch();
 const tick=()=>{
  progressTimer=0;
  if(closed||startupActive||!sessionToken)return;
  const sockets=carrierMode==='websocket-lanes'?Array.from(lanes.values(),lane=>lane.socket):[webSocket];
  if(sockets.some(socket=>socket&&socket.readyState===WebSocket.OPEN&&sendProgress(socket))){fail(new Error('websocket send progress timeout'));return}
  progressTimer=setTimeout(tick,1000);
 };
 progressTimer=setTimeout(tick,1000);
}
function resumeTransport(){
 if(closed||!nativeWelcomeSent||startupActive||recoveryPromise)return;
 fail(new Error('transport resumed after network interruption'));
}
const status=state=>{if(port&&!closed)port.postMessage({t:'status',state})};
const pause=(milliseconds,...signals)=>new Promise((resolve,reject)=>{
 const active=signals.filter(Boolean);
 if(active.some(signal=>signal.aborted)){reject(new Error('operation cancelled'));return}
 let timer=0,settled=false;
 const cleanup=()=>{if(timer)clearTimeout(timer);for(const signal of active)signal.removeEventListener('abort',abort)};
 const finish=(callback,value)=>{if(settled)return;settled=true;cleanup();callback(value)};
 const abort=()=>finish(reject,new Error('operation cancelled'));
 for(const signal of active)signal.addEventListener('abort',abort,{once:true});
 timer=setTimeout(()=>finish(resolve),milliseconds);
});
const options=(method,token,body,headers,signal,keepalive)=>({
 method,body,signal,keepalive:!!keepalive,mode:'same-origin',credentials:'omit',cache:'no-store',redirect:'error',referrerPolicy:'no-referrer',
 headers:Object.assign(token?{Authorization:'Bearer '+token}:{},body?{'Content-Type':'application/octet-stream'}:{},headers||{})
});
const bufferedBytes=()=>{
 let total=webSocket&&webSocket.readyState===WebSocket.OPEN?webSocket.bufferedAmount:0;
 if(carrierMode==='websocket-lanes')for(const lane of lanes.values())if(lane.socket&&lane.socket.readyState===WebSocket.OPEN)total+=lane.socket.bufferedAmount;
 return total;
};
function reserve(data,lane){
 if(!data.byteLength||queuedBytes+bufferedBytes()>queueLimit-data.byteLength||queuedItems>=queueItemLimit)return false;
 if(lane&&(lane.bytes>laneQueueLimit-data.byteLength||lane.items>=laneItemLimit))return false;
 queuedBytes+=data.byteLength;queuedItems++;
 if(lane){lane.bytes+=data.byteLength;lane.items++}
 return true;
}
function release(bytes,items,lane){
 queuedBytes-=bytes;queuedItems-=items;
 if(lane){lane.bytes-=bytes;lane.items-=items}
}
function splitFrames(value,copyData=true){
 const view=new DataView(value),result=[];let offset=0;
 while(offset<value.byteLength){
  if(value.byteLength-offset<8||result.length>=4096)throw new Error('invalid frame batch');
  const type=view.getUint8(offset),id=(view.getUint8(offset+1)<<16)|(view.getUint8(offset+2)<<8)|view.getUint8(offset+3);
  const size=view.getUint32(offset+4),end=offset+8+size;
  if((type===2&&!size)||size>1048576||end>value.byteLength)throw new Error('invalid frame');
  result.push({type,id,offset,end,data:copyData?(offset===0&&end===value.byteLength?value:value.slice(offset,end)):null});offset=end;
 }
 if(!result.length)throw new Error('empty frame batch');
 return result;
}
function frameBound(value,maxFrames,maxBytes){
 const view=new DataView(value);let offset=0,frames=0;
 while(offset<value.byteLength){
  if(value.byteLength-offset<8)throw new Error('invalid frame batch');
  const size=view.getUint32(offset+4),end=offset+8+size;
  if(end>value.byteLength)throw new Error('invalid frame');
  if(frames>0&&(frames>=maxFrames||end>maxBytes))break;
  frames++;offset=end;
 }
 return {frames,bytes:offset};
}
function joinPending(values,lane){
 let total=0,count=0,frames=0;
 while(count<values.length){
  const bound=frameBound(values[count],4096,batchLimit);
  const whole=bound.bytes===values[count].byteLength;
  if(count===0&&!whole){
   const head=new Uint8Array(values[0],0,bound.bytes).slice();
   values[0]=values[0].slice(bound.bytes);
   queuedItems++;if(lane)lane.items++;
   return {body:head.buffer,total:bound.bytes,count:1};
  }
  if(count!==0&&(total+values[count].byteLength>batchLimit||frames+bound.frames>4096))break;
  total+=values[count].byteLength;frames+=bound.frames;count++;
 }
 const joined=new Uint8Array(total);let offset=0;
 for(const data of values.splice(0,count)){joined.set(new Uint8Array(data),offset);offset+=data.byteLength}
 return {body:joined.buffer,total,count};
}
function retryAfterMs(response){
 const header=response.headers.get('Retry-After');
 if(!header)return 0;
 const seconds=Number(header);
 if(Number.isFinite(seconds)&&seconds>=0)return Math.min(seconds*1000,30000);
 const when=Date.parse(header);
 if(Number.isFinite(when)){const delta=when-Date.now();return delta>0?Math.min(delta,30000):0}
 return 0;
}
function discardResponse(response){
 try{if(response?.body&&!response.body.locked)response.body.cancel().catch(()=>{})}catch(error){}
}
function invalidResponse(reason){const error=new Error(reason);error.retryable=false;return error}
async function readResponse(response,limit,exact,signal,deadline){
 const declared=response.headers.get('Content-Length');
 if(declared!==null&&(!/^\d+$/.test(declared)||Number(declared)>limit))throw invalidResponse('response body exceeds limit');
 if(!response.body||typeof response.body.getReader!=='function')throw invalidResponse('streamed response body is unavailable');
 const reader=response.body.getReader(),chunks=[];let total=0;
 try{
  while(true){
   if(signal.aborted||Date.now()>=deadline)throw new Error('response deadline reached');
   const result=await reader.read();
   if(signal.aborted||Date.now()>=deadline)throw new Error('response deadline reached');
   if(result.done)break;
   total+=result.value.byteLength;
   if(total>limit)throw invalidResponse('response body exceeds limit');
   chunks.push(result.value);
  }
  if(exact&&total!==limit)throw invalidResponse('invalid response body size');
  const body=new Uint8Array(total);let offset=0;
  for(const chunk of chunks){body.set(chunk,offset);offset+=chunk.byteLength}
  return body.buffer;
 }catch(error){reader.cancel().catch(()=>{});throw error}
 finally{reader.releaseLock()}
}
async function request(path,makeOptions,onHeaders,epoch=recoveryEpoch){
 let delay=250,attempt=0;
 const deadline=Math.min(Date.now()+requestTimeoutMs,startupActive?startupDeadline:Infinity);
 while(true){
  if(closed||epoch!==recoveryEpoch)throw new Error('operation cancelled');
  const remaining=deadline-Date.now();
  if(remaining<=0)throw new Error('carrier request deadline reached');
  const requestOptions=makeOptions(),controller=new AbortController();
  const external=requestOptions.signal,abort=()=>controller.abort(),cancel=()=>controller.abort();
  const lifecycle=lifecycleController;
  lifecycle.signal.addEventListener('abort',cancel,{once:true});
  if(external)external.addEventListener('abort',abort,{once:true});
  requestOptions.signal=controller.signal;
  const timer=setTimeout(abort,remaining);
  let wait=0,serviceUnavailable=false,response=null;
  try{
   response=await fetch(relayOrigin+path,requestOptions);
   if(closed||epoch!==recoveryEpoch||controller.signal.aborted)throw new Error('operation cancelled');
   if([408,429,502,503,504].includes(response.status)){serviceUnavailable=true;wait=retryAfterMs(response);discardResponse(response)}
   else if(response.status===200&&(path==='/api/v1/session'||path==='/api/v1/down')){
    if(onHeaders)onHeaders(response);
    const body=await readResponse(response,path==='/api/v1/session'?8:batchLimit,path==='/api/v1/session',controller.signal,deadline);
    return {status:response.status,headers:response.headers,arrayBuffer:async()=>body};
   }else{
    discardResponse(response);
    return {status:response.status,headers:response.headers,arrayBuffer:async()=>new ArrayBuffer(0)};
   }
  }catch(error){
   discardResponse(response);
   if(error.retryable===false)throw error;
   if(closed||epoch!==recoveryEpoch||(external&&external.aborted)||lifecycle.signal.aborted)throw error;
   if(Date.now()>=deadline)throw new Error('carrier request deadline reached');
   attempt++;if(attempt===9)throw new Error('carrier retry limit reached');
  }
  finally{clearTimeout(timer);lifecycle.signal.removeEventListener('abort',cancel);if(external)external.removeEventListener('abort',abort)}
  if(Date.now()>=deadline)throw new Error('carrier request deadline reached');
  status('reconnecting');
  const backoff=Math.min(wait||(delay+Math.floor(Math.random()*Math.max(1,delay/4))),deadline-Date.now());
  await pause(backoff,lifecycle.signal,external);
  if(!serviceUnavailable)delay=Math.min(delay*2,5000);
 }
}
function fail(error){
 if(closed)return;
 const message=error&&typeof error.message==='string'?error.message:'';
 const reason=/deadline|timed out|timeout|retry limit/i.test(message)?'timeout':/session creation|session rejected/i.test(message)?'session_rejected':/websocket|socket/i.test(message)?'websocket_failed':/welcome|frame|protocol/i.test(message)?'protocol_error':/body|response/i.test(message)?'body_failed':'transport_error';
 const phase=startupActive?'startup':'runtime';
 const fallbackOnRecovery=configuredCarrierMode==='auto'&&phase==='startup'&&carrierMode==='websocket'&&/websocket|socket/i.test(message);
 if(recoveryPromise)return;
 const epoch=recoveryEpoch+1;
 recoveryEpoch=epoch;
 let task;
 task=Promise.resolve().then(()=>recover(fallbackOnRecovery)).catch(()=>{
  if(closed||recoveryEpoch!==epoch)return;
  status('failed');if(port)port.postMessage({t:'close'});close(true);
 }).finally(()=>{
  if(recoveryPromise===task)recoveryPromise=null;
 });
 recoveryPromise=task;
 reportFailure(phase,reason);
}
function reportFailure(phase,reason){
 try{fetch(relayOrigin+'/api/v1/report',{method:'POST',mode:'same-origin',credentials:'omit',cache:'no-store',keepalive:true,referrerPolicy:'no-referrer',headers:{Authorization:'Bearer '+bootstrap,'Content-Type':'application/json'},body:JSON.stringify({phase,reason})}).catch(()=>{})}catch(error){}
}
function retireNativeStreams(){
 if(port&&!closed)for(const id of activeNativeStreams){const frame=closeFrame(id);port.postMessage(frame,[frame])}
 activeNativeStreams.clear();
}
function trackNativeStreams(data){
 const kept=[];let dropped=false;
 for(const frame of splitFrames(data,false)){
  if([2,3,4].includes(frame.type)&&!activeNativeStreams.has(frame.id)){dropped=true;continue}
  if(frame.type===1)activeNativeStreams.add(frame.id);
  else if(frame.type===3)activeNativeStreams.delete(frame.id);
  kept.push(frame);
 }
 if(!kept.length)return null;
 if(!dropped)return data;
 const joined=new Uint8Array(kept.reduce((size,frame)=>size+frame.end-frame.offset,0));
 let offset=0;for(const frame of kept){const size=frame.end-frame.offset;joined.set(new Uint8Array(data,frame.offset,size),offset);offset+=size}
 return joined.buffer;
}
function trackRemoteClosed(data){
 for(const frame of splitFrames(data,false))if(frame.type===3)activeNativeStreams.delete(frame.id);
}
function resetIncarnation(){
 stopProgressWatch();
 lifecycleController.abort();
 if(pollController)pollController.abort();
 if(webSocketTimer)clearTimeout(webSocketTimer);webSocketTimer=0;
 if(webSocket)try{webSocket.close()}catch(error){}webSocket=null;
 for(const lane of lanes.values()){
  if(lane.controller)lane.controller.abort();if(lane.timer)clearTimeout(lane.timer);if(lane.openTimer)clearTimeout(lane.openTimer);
  if(lane.socket)try{lane.socket.close()}catch(error){}
 }
 for(const item of pending)release(item.byteLength,1,null);
 for(const item of upPending)release(item.byteLength,1,null);
 pending.length=0;upPending.length=0;lanes.clear();closedLanes.clear();closedLaneOrder.length=0;
 queuedBytes=0;queuedItems=0;pollController=null;upRunning=false;
 sessionToken='';upSequence=1;downCursor='0';
 lifecycleController=new AbortController();startupDeadline=0;startupActive=false;
}
async function recover(fallbackToHttps=false){
 if(closed)return;
 const recoveryDeadline=startupActive?startupDeadline:Date.now()+startupBudgetMs;
 if(fallbackToHttps&&carrierModes[carrierModeIndex+1]==='https'){carrierModeIndex++;carrierMode='https'}
 const oldToken=sessionToken;
 status('reconnecting');
 retireNativeStreams();
 recoveryEpoch++;
 const epoch=recoveryEpoch;
 resetIncarnation();
 startupActive=true;startupDeadline=recoveryDeadline;
 try{
  const controller=new AbortController(),remaining=startupDeadline-Date.now(),lifecycle=lifecycleController;
  if(remaining<=0)throw new Error('recovery deadline reached');
  const abort=()=>controller.abort();
  lifecycle.signal.addEventListener('abort',abort,{once:true});
  const timer=setTimeout(()=>controller.abort(),remaining);
  let response,raw;
  try{
   response=await fetch(relayOrigin+'/api/v1/recover',{method:'POST',signal:controller.signal,mode:'same-origin',credentials:'omit',cache:'no-store',redirect:'error',referrerPolicy:'no-referrer',headers:Object.assign({'X-Bridge-Capability':bridgeCapability},oldToken?{Authorization:'Bearer '+oldToken}:{})});
   if(response.status===200){const bytes=await readResponse(response,1024,false,controller.signal,startupDeadline);raw=new TextDecoder().decode(bytes)}
  }finally{clearTimeout(timer);lifecycle.signal.removeEventListener('abort',abort);discardResponse(response)}
  if(Date.now()>=startupDeadline)throw new Error('recovery deadline reached');
  if(closed||recoveryEpoch!==epoch)return;
  if(response.status!==200)throw new Error('recovery rejected');
  const body=JSON.parse(raw);
  if(!body||typeof body.bootstrap!=='string'||body.bootstrap.length<32||body.bootstrap.length>128)throw new Error('invalid recovery response');
  if(closed||recoveryEpoch!==epoch)return;
  const hello=new Uint8Array([16,0,0,0,0,0,0,1,1]).buffer;
  await createSession(hello,body.bootstrap,true);
 }catch(error){
  if(closed)return;
  if(recoveryEpoch!==epoch)return;
  // Retry the established carrier once; only a failed WS opening permits
  // auto mode to move to HTTPS. Both incarnations share the same deadline.
  if(configuredCarrierMode==='auto'&&carrierMode==='websocket'&&error.carrierOpeningFailed&&Date.now()<recoveryDeadline){await recover(true);return}
  status('failed');if(port)port.postMessage({t:'close'});close(true);
 }
}
async function createSession(first,bootstrapToken=bootstrap,recovery=false){
 let welcomePosted=false,createdToken='';
 const epoch=recoveryEpoch;
 try{
  status('connecting');
  const response=await request('/api/v1/session',()=>options('POST',bootstrapToken,first,configuredCarrierMode==='auto'?{'X-Carrier-Mode':carrierMode}:undefined),headers=>{
   createdToken=headers.headers.get('X-Session-Token')||'';
   if(!closed&&epoch===recoveryEpoch){sessionToken=createdToken;downCursor=headers.headers.get('X-Down-Cursor')||'0'}
  },epoch);
  if(response.status!==200||response.headers.get('X-Carrier-Mode')!==carrierMode)throw new Error('session creation rejected');
  if(!sessionToken)throw new Error('missing session token');
  if(closed||epoch!==recoveryEpoch){if(createdToken)fetch(relayOrigin+'/api/v1/session',options('DELETE',createdToken,null,null,undefined,true)).catch(()=>{});return}
  const welcome=await response.arrayBuffer();
  if(new Uint8Array(welcome).some((byte,index)=>byte!==(index===0?17:0)))throw invalidResponse('invalid WELCOME frame');
  if(carrierMode==='websocket')await openWebSocket();
  if(closed||epoch!==recoveryEpoch){if(createdToken)fetch(relayOrigin+'/api/v1/session',options('DELETE',createdToken,null,null,undefined,true)).catch(()=>{});return}
  startupActive=false;
  if(!nativeWelcomeSent){nativeWelcomeSent=true;port.postMessage(welcome,[welcome])}
  welcomePosted=true;
  if(recovery)recoveryPromise=null;
  status('connected');
  watchProgress();
  for(const data of pending.splice(0)){release(data.byteLength,1,null);queueCarrier(data)}
  if(carrierMode==='https')poll();
  else if(carrierMode==='https-lanes')pollLane(ensureLane(0));
 }catch(error){if(!closed&&epoch===recoveryEpoch&&!welcomePosted){if(recovery)throw error;fail(error)}}
}
function queueCarrier(data){
 try{
  if(carrierMode==='https')queueUp(data);
  else if(carrierMode==='https-lanes')for(const value of splitFrames(data))queueLane(value);
  else if(carrierMode==='websocket')queueWebSocket(data);
  else for(const value of splitFrames(data))queueWebSocketLane(value);
 }catch(error){fail(error)}
}
function queueUp(data){
 if(!reserve(data,null)){fail();return}
 upPending.push(data);runUp();
}
async function runUp(){
 const epoch=recoveryEpoch;
 if(upRunning)return;
 upRunning=true;
 try{
  while(!closed&&sessionToken&&upPending.length){
   const batch=joinPending(upPending,null),sequence=String(upSequence);
   const response=await request('/api/v1/up',()=>options('POST',sessionToken,batch.body,{'X-Up-Seq':sequence}),undefined,epoch);
   if(closed||epoch!==recoveryEpoch)return;
   if(response.status!==204||response.headers.get('X-Up-Ack')!==sequence)throw new Error('uplink rejected');
   release(batch.total,batch.count,null);port.postMessage({t:'traffic',up:batch.total,down:0});upSequence++;
  }
 }catch(error){if(!closed&&epoch===recoveryEpoch)fail(error)}
 finally{if(epoch===recoveryEpoch){upRunning=false;if(!closed&&sessionToken&&upPending.length)runUp()}}
}
async function poll(){
 const epoch=recoveryEpoch;
 while(!closed&&epoch===recoveryEpoch&&sessionToken){
  try{
   pollController=new AbortController();
   const response=await request('/api/v1/down',()=>options('POST',sessionToken,null,{'X-Down-Cursor':downCursor},pollController.signal),undefined,epoch);
   if(closed||epoch!==recoveryEpoch)return;
   if(response.status===204){status('connected');continue}
   if(response.status!==200)throw new Error('downlink rejected');
   const next=response.headers.get('X-Down-Cursor')||'',data=await response.arrayBuffer();
   if(!next||!data.byteLength)throw new Error('invalid downlink response');
   if(closed||epoch!==recoveryEpoch)return;
   trackRemoteClosed(data);port.postMessage({t:'traffic',up:0,down:data.byteLength});port.postMessage(data,[data]);downCursor=next;status('connected');
  }catch(error){if(!closed&&epoch===recoveryEpoch)fail(error);return}
 }
}
function ensureLane(id){
 let lane=lanes.get(id);
 if(!lane){lane={id,sequence:1,cursor:'0',pending:[],bytes:0,items:0,running:false,polling:false,socket:null,timer:0,openTimer:0,opened:false,localClosed:false,remoteClosed:false,finished:false};lanes.set(id,lane)}
 return lane;
}
function rememberLaneClosed(id){
 if(closedLanes.has(id))return;
 closedLanes.add(id);closedLaneOrder.push(id);
 if(closedLaneOrder.length>closedLaneLimit){const oldest=closedLaneOrder.shift();closedLanes.delete(oldest)}
}
function queueLane(value){
 let lane=lanes.get(value.id);
 if(!lane&&(value.type===2||value.type===3||value.type===4))return;
 if((!value.id&&!lane)||(!lane&&closedLanes.has(value.id)))throw new Error('closed lane was reused');
 if(!lane&&value.type!==1)throw new Error('lane did not begin with OPEN');
 lane=lane||ensureLane(value.id);
 if(!reserve(value.data,lane)){fail();return}
 lane.pending.push(value.data);if(value.type===3)lane.localClosed=true;
 runLaneUp(lane);
}
async function runLaneUp(lane){
 const epoch=recoveryEpoch;
 if(lane.running)return;
 lane.running=true;
 try{
  while(!closed&&sessionToken&&lane.pending.length){
   const batch=joinPending(lane.pending,lane),sequence=String(lane.sequence),laneID=String(lane.id);
   const response=await request('/api/v1/up',()=>options('POST',sessionToken,batch.body,{'X-Up-Seq':sequence,'X-Lane-ID':laneID}),undefined,epoch);
   if(closed||epoch!==recoveryEpoch)return;
   if(response.status!==204||response.headers.get('X-Up-Ack')!==sequence)throw new Error('lane uplink rejected');
   release(batch.total,batch.count,lane);port.postMessage({t:'traffic',up:batch.total,down:0});lane.sequence++;
   if(!lane.polling)pollLane(lane);
  }
 }catch(error){if(!closed&&epoch===recoveryEpoch)fail(error)}
 finally{lane.running=false;if(!closed&&epoch===recoveryEpoch&&sessionToken&&lane.pending.length)runLaneUp(lane)}
}
async function pollLane(lane){
 const epoch=recoveryEpoch;
 if(lane.polling)return;
 lane.polling=true;
 try{
  while(!closed&&sessionToken&&lanes.get(lane.id)===lane){
   const controller=new AbortController(),laneID=String(lane.id);
   lane.controller=controller;
   const response=await request('/api/v1/down',()=>options('POST',sessionToken,null,{'X-Down-Cursor':lane.cursor,'X-Lane-ID':laneID},controller.signal),undefined,epoch);
   if(closed||epoch!==recoveryEpoch)return;
   if(response.status===204){
    if(response.headers.get('X-Lane-Closed')==='1'){lanes.delete(lane.id);rememberLaneClosed(lane.id);return}
    status('connected');continue;
   }
   if(response.status!==200)throw new Error('lane downlink rejected');
   const next=response.headers.get('X-Down-Cursor')||'',data=await response.arrayBuffer();
   if(!next||!data.byteLength)throw new Error('invalid lane downlink response');
   for(const value of splitFrames(data))if(value.id!==lane.id)throw new Error('cross-lane frame');
   if(closed||epoch!==recoveryEpoch)return;
   trackRemoteClosed(data);port.postMessage({t:'traffic',up:0,down:data.byteLength});port.postMessage(data,[data]);lane.cursor=next;status('connected');
  }
 }catch(error){if(!closed&&epoch===recoveryEpoch)fail(error)}
 finally{lane.polling=false;lane.controller=null}
}
function openWebSocket(){
 return new Promise((resolve,reject)=>{
  const epoch=recoveryEpoch,lifecycle=lifecycleController;
  const timeout=Math.min(webSocketOpenTimeoutMs,startupActive?startupDeadline-Date.now():webSocketOpenTimeoutMs);
  if(timeout<=0){reject(new Error('websocket opening deadline reached'));return}
  const target=relayOrigin.replace(/^https:/,'wss:')+'/api/v1/ws',socket=new WebSocket(target,'tproxy-v1.'+sessionToken);
  let settled=false;
  const finish=(callback,value)=>{if(settled)return;settled=true;clearTimeout(timer);lifecycle.signal.removeEventListener('abort',onAbort);if(callback===reject&&value)value.carrierOpeningFailed=true;callback(value)};
  const onAbort=()=>{finish(reject,new Error('websocket opening cancelled'));socket.close()};
  const timer=setTimeout(()=>{finish(reject,new Error('websocket opening deadline reached'));socket.close()},timeout);
  lifecycle.signal.addEventListener('abort',onAbort,{once:true});
  webSocket=socket;socket.binaryType='arraybuffer';
  socket.onopen=()=>{if(settled||closed||epoch!==recoveryEpoch){socket.close();return}finish(resolve)};
  socket.onmessage=event=>{
   if(closed||epoch!==recoveryEpoch)return;
   if(!(event.data instanceof ArrayBuffer)||!event.data.byteLength){fail();return}
   try{trackRemoteClosed(event.data)}catch(error){fail(error);return}
   port.postMessage({t:'traffic',up:0,down:event.data.byteLength});port.postMessage(event.data,[event.data]);status('connected');
  };
  socket.onerror=()=>finish(reject,new Error('websocket failed'));
  socket.onclose=()=>{if(!settled)finish(reject,new Error('websocket closed before opening'));else if(!closed&&epoch===recoveryEpoch)fail(new Error('websocket closed'))};
 });
}
function queueWebSocket(data){
 if(!reserve(data,null)){fail();return}
 upPending.push(data);runWebSocketUp();
}
function runWebSocketUp(){
 if(closed||!webSocket||webSocket.readyState!==WebSocket.OPEN||!upPending.length)return;
 if(webSocket.bufferedAmount+queuedBytes>queueLimit||webSocket.bufferedAmount>=batchLimit){
  if(!webSocketTimer)webSocketTimer=setTimeout(()=>{webSocketTimer=0;runWebSocketUp()},10);
  return;
 }
 try{
  const batch=joinPending(upPending,null);webSocket.send(batch.body);sendProgress(webSocket,batch.total);release(batch.total,batch.count,null);
  port.postMessage({t:'traffic',up:batch.total,down:0});
  if(upPending.length)queueMicrotask(runWebSocketUp);
 }catch(error){fail(error)}
}
function closeFrame(id){
 const data=new ArrayBuffer(8),view=new DataView(data);
 view.setUint8(0,3);view.setUint8(1,id>>>16);view.setUint8(2,id>>>8);view.setUint8(3,id);view.setUint32(4,0);
 return data;
}
function finishWebSocketLane(lane,notify){
 if(lane.finished||lanes.get(lane.id)!==lane)return;
 activeNativeStreams.delete(lane.id);
 lane.finished=true;if(lane.timer)clearTimeout(lane.timer);if(lane.openTimer)clearTimeout(lane.openTimer);
 if(lane.bytes||lane.items)release(lane.bytes,lane.items,lane);
 lane.pending.length=0;lanes.delete(lane.id);rememberLaneClosed(lane.id);
 if(lane.socket&&(lane.socket.readyState===WebSocket.OPEN||lane.socket.readyState===WebSocket.CONNECTING))try{lane.socket.close()}catch(error){}
 if(notify&&port&&!closed){const frame=closeFrame(lane.id);port.postMessage(frame,[frame])}
}
function openWebSocketLane(lane){
 const epoch=recoveryEpoch;
 const target=relayOrigin.replace(/^https:/,'wss:')+'/api/v1/ws';
 const socket=new WebSocket(target,'tproxy-lane-v1.'+sessionToken+'.'+lane.id);
 lane.openTimer=setTimeout(()=>finishWebSocketLane(lane,true),webSocketOpenTimeoutMs);
 lane.socket=socket;socket.binaryType='arraybuffer';
 socket.onopen=()=>{if(closed||epoch!==recoveryEpoch||lane.finished){socket.close();return}clearTimeout(lane.openTimer);lane.openTimer=0;lane.opened=true;status('connected');runWebSocketLaneUp(lane)};
 socket.onmessage=event=>{
  if(closed||epoch!==recoveryEpoch||lane.finished)return;
  if(!(event.data instanceof ArrayBuffer)||!event.data.byteLength){fail();return}
  let values;try{values=splitFrames(event.data)}catch(error){fail(error);return}
  if(values.some(value=>value.id!==lane.id)){fail();return}
  if(values.some(value=>value.type===3))lane.remoteClosed=true;
  trackRemoteClosed(event.data);port.postMessage({t:'traffic',up:0,down:event.data.byteLength});port.postMessage(event.data,[event.data]);status('connected');
 };
 socket.onerror=()=>{};
 socket.onclose=()=>{
  if(closed||epoch!==recoveryEpoch||lane.finished)return;
  if(!lane.opened){finishWebSocketLane(lane,true);return}
  finishWebSocketLane(lane,!lane.localClosed&&!lane.remoteClosed);
 };
}
function queueWebSocketLane(value){
 let lane=lanes.get(value.id);
 if(!lane&&(value.type===2||value.type===3||value.type===4))return;
 if(!value.id||(!lane&&closedLanes.has(value.id)))throw new Error('closed lane was reused');
 if(!lane&&value.type!==1)throw new Error('lane did not begin with OPEN');
 lane=lane||ensureLane(value.id);
 if(value.type===3&&!lane.opened){finishWebSocketLane(lane,false);return}
 if(!reserve(value.data,lane)){fail();return}
 lane.pending.push(value.data);if(value.type===3)lane.localClosed=true;
 if(!lane.socket)openWebSocketLane(lane);else runWebSocketLaneUp(lane);
}
function runWebSocketLaneUp(lane){
 const socket=lane.socket;
 if(closed||lane.finished||!socket||socket.readyState!==WebSocket.OPEN||!lane.pending.length)return;
 if(socket.bufferedAmount>=batchLimit){
  if(!lane.timer)lane.timer=setTimeout(()=>{lane.timer=0;runWebSocketLaneUp(lane)},10);
  return;
 }
 try{
  const batch=joinPending(lane.pending,lane);socket.send(batch.body);sendProgress(socket,batch.total);release(batch.total,batch.count,lane);
  port.postMessage({t:'traffic',up:batch.total,down:0});
  if(lane.pending.length)queueMicrotask(()=>runWebSocketLaneUp(lane));
 }catch(error){fail(error)}
}
function close(notifyServer){
 if(closed)return;
 closed=true;
 stopProgressWatch();
 recoveryEpoch++;recoveryPromise=null;
 lifecycleController.abort();
 if(pollController)pollController.abort();
 for(const lane of lanes.values()){
  if(lane.controller)lane.controller.abort();
  if(lane.timer)clearTimeout(lane.timer);
  if(lane.openTimer)clearTimeout(lane.openTimer);
  if(lane.socket)try{lane.socket.close()}catch(error){}
 }
 if(webSocketTimer)clearTimeout(webSocketTimer);
 if(webSocket)webSocket.close();
 if(notifyServer&&sessionToken)fetch(relayOrigin+'/api/v1/session',options('DELETE',sessionToken,null,null,undefined,true)).catch(()=>{});
 pending.length=0;upPending.length=0;lanes.clear();activeNativeStreams.clear();closedLanes.clear();closedLaneOrder.length=0;
 queuedBytes=0;queuedItems=0;
 if(port)port.close();
}
function activatePort(nextPort){
 initialized=true;port=nextPort;
 startupActive=true;startupDeadline=Date.now()+startupBudgetMs;
 port.onmessage=message=>{
  if(closed)return;
  if(message.data instanceof ArrayBuffer){
   let data;try{data=trackNativeStreams(message.data)}catch(error){fail(error);return}
   if(!data)return;
   if(!createStarted){createStarted=true;createSession(data)}
   else if(!sessionToken){if(!reserve(data,null)){fail();return}pending.push(data)}
   else queueCarrier(data);
  }else if(message.data&&message.data.t==='close')close(true);
 };
 port.start();status('connecting');
}
addEventListener('message',event=>{
 if(initialized||event.source!==parent||event.data===null||typeof event.data!=='object')return;
 const keys=Object.keys(event.data).sort();
 if(keys.length!==2||keys[0]!=='t'||keys[1]!=='v'||event.data.t!=='tproxy-init'||event.data.v!==1||event.ports.length!==1)return;
 let source;try{source=new URL(event.origin)}catch(error){return}
 if(source.protocol!=='http:'||source.hostname!=='127.0.0.1'||!source.port||source.origin!==event.origin)return;
 activatePort(event.ports[0]);
},{once:false});
const androidBridge=globalThis.TelegramWebProxy;
if(!initialized&&androidNonce&&androidBridge&&typeof androidBridge.postMessage==='function'){
 const androidPort={onmessage:null,start(){},close(){androidBridge.onmessage=null},postMessage(value){
  if(value instanceof ArrayBuffer){
   let frames;try{frames=splitFrames(value)}catch(error){fail(error);return}
   for(const frame of frames)androidBridge.postMessage(frame.data);
  }else androidBridge.postMessage(JSON.stringify(value));
 }};
 androidBridge.onmessage=event=>{
  let data=event.data;if(typeof data==='string'){try{data=JSON.parse(data)}catch(error){return}}
  if(androidPort.onmessage)androidPort.onmessage({data});
 };
 activatePort(androidPort);androidBridge.postMessage(JSON.stringify({t:'tproxy-android-init',v:1,nonce:androidNonce}));
}
addEventListener('pagehide',()=>close(true),{once:true});
addEventListener('offline',()=>{wasOffline=true});
addEventListener('online',()=>{if(wasOffline){wasOffline=false;resumeTransport()}});
if(typeof document!=='undefined')document.addEventListener('visibilitychange',()=>{
 if(document.visibilityState==='hidden'){hiddenSince=Date.now();return}
 const paused=hiddenSince===null?0:Date.now()-hiddenSince;hiddenSince=null;
 if(paused>=requestTimeoutMs)resumeTransport();
});
})();
