// 腾讯位置服务 MCP 连通性诊断
// 用法: node mcp-tencent-map-diagnose.mjs
// 1) 直接校验 ~/.workbuddy/mcp.json 里当前配置的 URL 是否真的可用（这是重点）
// 2) 分别实测 Streamable HTTP 与旧版 SSE 两种传输的握手耗时
// 用来定位 MCP error -32001 请求超时究竟出在连接层、传输层还是服务端。

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const TIMEOUT = Number(process.env.MCP_DIAG_TIMEOUT || 30000);
const cfgPath = path.join(os.homedir(), '.workbuddy', 'mcp.json');

let configuredUrl = '', key = '';
try {
  const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
  configuredUrl = cfg?.mcpServers?.['tencent-map']?.url || '';
  key = /[?&]key=([^&]+)/.exec(configuredUrl)?.[1] || '';
} catch (e) {
  console.log('[FAIL] 无法读取 ' + cfgPath + ' : ' + e.message);
  process.exit(1);
}
if (!key) {
  console.log('[FAIL] mcp.json 里没找到 tencent-map 的 key');
  process.exit(1);
}
console.log('配置文件 : ' + cfgPath);
console.log('Key      : ' + key.slice(0, 6) + '****' + key.slice(-4) + ' (已脱敏)');
console.log('超时阈值 : ' + TIMEOUT + 'ms');
console.log('-'.repeat(68));

const HTTP_URL = `https://mcp.map.qq.com/mcp?key=${key}&format=0`;
const SSE_URL = `https://mcp.map.qq.com/sse?key=${key}&format=0`;

const reqInit = (id) => ({
  jsonrpc: '2.0', id, method: 'initialize',
  params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'wb-diag', version: '1.0' } },
});
const reqTools = (id) => ({ jsonrpc: '2.0', id, method: 'tools/list', params: {} });

function withTimeout(ms) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(new Error('timeout after ' + ms + 'ms')), ms);
  return { signal: ac.signal, done: () => clearTimeout(timer) };
}

async function readRpc(res, wantId, budgetMs) {
  const ct = (res.headers.get('content-type') || '').toLowerCase();
  if (ct.includes('application/json')) return await res.json();
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  const end = Date.now() + budgetMs;
  let buf = '';
  try {
    while (Date.now() < end) {
      const r = await reader.read();
      if (r.done) break;
      buf += dec.decode(r.value, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (!line.startsWith('data:')) continue;
        const raw = line.slice(5).trim();
        if (!raw) continue;
        try {
          const j = JSON.parse(raw);
          if (wantId === undefined || j.id === wantId) return j;
        } catch { /* 分片未完整，继续读 */ }
      }
    }
  } finally {
    try { await reader.cancel(); } catch {}
  }
  throw new Error('在 ' + budgetMs + 'ms 内未收到 id=' + wantId + ' 的响应');
}

async function readSseNamedEvent(res, wantEvent, budgetMs) {
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  const end = Date.now() + budgetMs;
  let buf = '', curEvent = null;
  try {
    while (Date.now() < end) {
      const r = await reader.read();
      if (r.done) break;
      buf += dec.decode(r.value, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (line.startsWith('event:')) curEvent = line.slice(6).trim();
        else if (line.startsWith('data:')) {
          const raw = line.slice(5).trim();
          if (curEvent === wantEvent && raw) return raw;
        } else if (line === '') curEvent = null;
      }
    }
  } finally {
    try { await reader.cancel(); } catch {}
  }
  throw new Error('未收到 event: ' + wantEvent);
}

const results = [];
function record(name, ok, ms, note) {
  results.push({ name, ok });
  console.log((ok ? '[OK]   ' : '[FAIL] ') + name.padEnd(30) + String(ms).padStart(7) + 'ms  ' + note);
}

async function probeStreamableHttp(url, label, withTools) {
  let t = Date.now();
  const g = withTimeout(TIMEOUT);
  let sid = null;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' },
      body: JSON.stringify(reqInit(1)),
      signal: g.signal,
    });
    sid = res.headers.get('mcp-session-id');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const j = await readRpc(res, 1, TIMEOUT);
    if (j.error) throw new Error(JSON.stringify(j.error));
    const info = j.result?.serverInfo || {};
    record(label + ' initialize', true, Date.now() - t,
      (info.name || '?') + ' v' + (info.version || '') + '  session=' + (sid ? 'yes' : 'no'));
  } catch (e) {
    record(label + ' initialize', false, Date.now() - t, e.message);
    return;
  } finally { g.done(); }

  if (!withTools) return;
  t = Date.now();
  const g2 = withTimeout(TIMEOUT);
  try {
    const res2 = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json, text/event-stream',
        ...(sid ? { 'mcp-session-id': sid } : {}),
      },
      body: JSON.stringify(reqTools(2)),
      signal: g2.signal,
    });
    const j2 = await readRpc(res2, 2, TIMEOUT);
    if (j2.error) throw new Error(JSON.stringify(j2.error));
    const tools = j2.result?.tools || [];
    record(label + ' tools/list', true, Date.now() - t,
      tools.length + ' 个工具: ' + tools.map(x => x.name).slice(0, 6).join(', '));
  } catch (e) {
    record(label + ' tools/list', false, Date.now() - t, e.message);
  } finally { g2.done(); }
}

async function probeSse(url, label) {
  let t = Date.now();
  let endpoint = null;
  const g = withTimeout(TIMEOUT);
  try {
    const res = await fetch(url, { headers: { Accept: 'text/event-stream' }, signal: g.signal });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const ep = await readSseNamedEvent(res, 'endpoint', TIMEOUT);
    endpoint = ep.startsWith('http') ? ep : new URL(ep, 'https://mcp.map.qq.com').toString();
    record(label + ' 建立连接/取 endpoint', true, Date.now() - t, 'endpoint 已返回');
  } catch (e) {
    record(label + ' 建立连接/取 endpoint', false, Date.now() - t, e.message);
    return;
  } finally { g.done(); }

  t = Date.now();
  const g2 = withTimeout(TIMEOUT);
  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(reqInit(11)),
      signal: g2.signal,
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    record(label + ' initialize 投递', true, Date.now() - t, 'POST 已被接受');
  } catch (e) {
    record(label + ' initialize 投递', false, Date.now() - t, e.message);
  } finally { g2.done(); }
}

console.log('测试 1/3  校验 mcp.json 当前配置的 URL');
if (/\/mcp\b/.test(configuredUrl)) {
  await probeStreamableHttp(configuredUrl, '当前配置(/mcp)', false);
} else if (/\/sse\b/.test(configuredUrl)) {
  console.log('[NOTE] 当前仍在使用旧版 SSE 端点，长连接易被代理/网关掐断，是 -32001 的高发原因。');
  await probeSse(configuredUrl, '当前配置(/sse)');
} else {
  console.log('[FAIL] 无法识别的端点路径: ' + configuredUrl.replace(key, '****'));
}
console.log('');
console.log('测试 2/3  Streamable HTTP (/mcp，官方推荐)');
await probeStreamableHttp(HTTP_URL, 'StreamableHTTP', true);
console.log('');
console.log('测试 3/3  SSE (/sse，旧版传输)');
await probeSse(SSE_URL, 'SSE');

console.log('-'.repeat(68));
const okCount = results.filter(r => r.ok).length;
console.log('汇总: ' + okCount + '/' + results.length + ' 项通过');
const httpOk = results.find(r => r.name.startsWith('StreamableHTTP initialize'))?.ok;
const curOk = results.find(r => r.name.startsWith('当前配置'))?.ok;
if (httpOk && !curOk) console.log('结论: /mcp 可用但当前配置不通 —— 建议把 mcp.json 的 URL 换成 /mcp 端点。');
else if (httpOk) console.log('结论: 服务端与 key 均正常，当前配置可用。若客户端仍报 -32001，问题在本机网络出口/代理。');
else console.log('结论: /mcp 也不通，请检查 key 是否开启 WebServiceAPI 权限及配额。');
