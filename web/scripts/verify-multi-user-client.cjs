#!/usr/bin/env node
// Isolated regression checks: no browser, platform account or network required.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const ts = require('typescript');

const root = path.resolve(__dirname, '..');
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'onecity-client-check-'));
for (const name of ['session', 'sse']) {
  const source = fs.readFileSync(path.join(root, 'src/utils', `${name}.ts`), 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  });
  fs.writeFileSync(path.join(temporary, `${name}.js`), outputText);
}

global.window = new EventTarget();
const { notifyAuthorizationChanged, subscribeAuthorizationChange, buildDeerFlowDownloadUrl, isDeerFlowArtifactUrl } = require(path.join(temporary, 'session.js'));
const { SseClient } = require(path.join(temporary, 'sse.js'));

const flush = () => new Promise((resolve) => setImmediate(resolve));
const storageEvent = (key, oldValue, newValue) => {
  const event = new Event('storage');
  Object.assign(event, { key, oldValue, newValue });
  return event;
};

async function main() {
  let changes = 0;
  const unsubscribe = subscribeAuthorizationChange(() => changes++);
  window.dispatchEvent(storageEvent('theme', 'a', 'b'));
  assert.equal(changes, 0);
  window.dispatchEvent(storageEvent('Authorization', 'old-session', 'new-session'));
  assert.equal(changes, 1, 'Other-tab account changes must invalidate state');
  notifyAuthorizationChanged();
  assert.equal(changes, 2, 'Same-tab login/logout must invalidate state');
  unsubscribe();
  notifyAuthorizationChanged();
  assert.equal(changes, 2);

  const external = 'https://untrusted.example/report.csv';
  const proxy = new URL(buildDeerFlowDownloadUrl(external, 'dialog-a', 'https://platform.example'), 'https://platform.example');
  assert.equal(proxy.origin, 'https://platform.example');
  assert.equal(proxy.pathname, '/query/deerflow/file/download');
  assert.equal(proxy.searchParams.get('url'), external);
  const nested = `/query/deerflow/file/download?url=${encodeURIComponent('/api/threads/a/artifacts/report.csv')}&conversation_id=old-dialog`;
  const normalized = new URL(buildDeerFlowDownloadUrl(nested, 'current-dialog', 'https://platform.example'), 'https://platform.example');
  assert.equal(normalized.searchParams.get('url'), '/api/threads/a/artifacts/report.csv');
  assert.equal(normalized.searchParams.get('conversation_id'), 'current-dialog');
  assert(isDeerFlowArtifactUrl('/api/threads/a/artifacts/report.png', 'https://platform.example'));
  assert(isDeerFlowArtifactUrl('https://deerflow.example/api/threads/a/artifacts/report.csv', 'https://platform.example'));
  assert(!isDeerFlowArtifactUrl('https://untrusted.example/report.csv', 'https://platform.example'));

  let signal;
  global.fetch = async (_url, options) => {
    signal = options.signal;
    return { ok: true, body: { getReader: () => ({ read: () => new Promise(() => {}) }) } };
  };
  const client = new SseClient('/query/deerflow', { Authorization: 'synthetic-login' });
  client.connect(() => assert.fail('No frames expected'));
  await flush();
  assert.equal(client.connected, true);
  notifyAuthorizationChanged();
  assert.equal(signal.aborted, true, 'Login changes must close active streams');
  assert.equal(client.connected, false);

  let firstSignal;
  global.fetch = async (_url, options) => {
    firstSignal ||= options.signal;
    return { ok: true, body: { getReader: () => ({ read: () => new Promise(() => {}) }) } };
  };
  client.connect(() => {});
  await flush();
  client.connect(() => {});
  assert.equal(firstSignal.aborted, true, 'Reconnect must close previous fetch');
  client.close();

  global.fetch = async () => ({ ok: false, status: 429 });
  let error;
  client.connect(() => {}, (value) => { error = value; });
  await flush();
  assert.match(error.message, /并发名额已满/);
  assert.equal(client.connected, false);
  client.close();
  console.log('Multi-user client checks passed: session events, download token boundary, stream disposal/reconnect, quota errors.');
}

main().catch((error) => { console.error(error); process.exitCode = 1; }).finally(() => {
  fs.rmSync(temporary, { recursive: true, force: true });
});
