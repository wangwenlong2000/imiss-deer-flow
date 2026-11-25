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

const cityBrainSource = fs.readFileSync(path.join(root, 'src/pages/city-brain/index.tsx'), 'utf8');
const cityBrainTree = ts.createSourceFile('city-brain.tsx', cityBrainSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const normalizeStatement = cityBrainTree.statements.find((node) =>
  ts.isVariableStatement(node) && node.declarationList.declarations.some((declaration) => declaration.name.getText(cityBrainTree) === 'normalizeComplianceResult'));
assert(normalizeStatement, 'Use the actual page compliance normalizer');
const normalizedModule = ts.transpileModule(`${normalizeStatement.getText(cityBrainTree)}\nmodule.exports = normalizeComplianceResult;`, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
});
fs.writeFileSync(path.join(temporary, 'compliance-normalizer.js'), normalizedModule.outputText);
const normalizeComplianceResult = require(path.join(temporary, 'compliance-normalizer.js'));
const terminalStatement = cityBrainTree.statements.find((node) =>
  ts.isVariableStatement(node) && node.declarationList.declarations.some((declaration) => declaration.name.getText(cityBrainTree) === 'isTerminalTaskStatus'));
assert(terminalStatement);
const terminalModule = ts.transpileModule(`${terminalStatement.getText(cityBrainTree)}\nmodule.exports = isTerminalTaskStatus;`, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
});
fs.writeFileSync(path.join(temporary, 'terminal-status.js'), terminalModule.outputText);
const isTerminalTaskStatus = require(path.join(temporary, 'terminal-status.js'));
const loginSource = fs.readFileSync(path.join(root, 'src/pages/login/index.tsx'), 'utf8');
const loginTree = ts.createSourceFile('login.tsx', loginSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const loginRules = {};
function findLoginRules(node) {
  if (ts.isJsxOpeningElement(node) && node.tagName.getText(loginTree) === 'Form.Item') {
    const name = node.attributes.properties.find((attribute) => ts.isJsxAttribute(attribute) && attribute.name.getText(loginTree) === 'name');
    const rules = node.attributes.properties.find((attribute) => ts.isJsxAttribute(attribute) && attribute.name.getText(loginTree) === 'rules');
    if (name?.initializer && ts.isStringLiteral(name.initializer) && rules?.initializer && ts.isJsxExpression(rules.initializer)) {
      loginRules[name.initializer.text] = rules.initializer.expression.getText(loginTree);
    }
  }
  ts.forEachChild(node, findLoginRules);
}
findLoginRules(loginTree);
assert(loginRules.email && loginRules.password, 'Use the actual login Form.Item rules');

async function verifyLoginRules() {
  const Schema = require('@rc-component/async-validator').default;
  const compileRules = (source, title) => {
    const output = ts.transpileModule(`module.exports = ${source};`, {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
    }).outputText;
    const module = { exports: {} };
    new Function('module', 'title', 't', output)(module, title, (key) => key);
    return module.exports;
  };
  const schemaFor = (title) => new Schema({
    email: compileRules(loginRules.email, title),
    password: compileRules(loginRules.password, title),
  });
  const login = schemaFor('login');
  await login.validate({ email: `onecity-smoke-${'a'.repeat(32)}-a@smoke.invalid`, password: 'existing-short' });
  await login.validate({ email: 'city.user+review@sub-domain.example', password: 'existing-password-longer-than-sixteen-characters' });
  await assert.rejects(login.validate({ email: 'invalid-email', password: 'existing' }));
  await assert.rejects(login.validate({ email: 'valid@example.com', password: '' }));
  const registration = schemaFor('register');
  await registration.validate({ email: 'valid@example.com', password: 'GoodPass1@' });
  await assert.rejects(registration.validate({ email: 'valid@example.com', password: 'short' }));
  await assert.rejects(registration.validate({ email: 'hyphen-user@example.com', password: 'GoodPass1@' }));
}
let cancelHandler;
function findCancelHandler(node) {
  if (ts.isVariableDeclaration(node) && node.name.getText(cityBrainTree) === 'handleCancel') {
    cancelHandler = node.initializer;
  }
  ts.forEachChild(node, findCancelHandler);
}
findCancelHandler(cityBrainTree);
assert(cancelHandler, 'Use the actual page cancellation handler');

async function verifyUnconfirmedCancellation() {
  const effects = [];
  const client = { activeRun: { thread_id: 'thread-a', run_id: 'run-a' }, close: () => effects.push('closed') };
  const bindings = {
    eventSourceRef: { current: client }, isCurrentSession: () => true,
    setCancelLoading: (value) => effects.push(['loading', value]),
    currentDialogId: 'dialog-a', authorization: 'synthetic-login',
    isTerminalTaskStatus,
    setSendLoading: () => effects.push('send-cleared'),
    setRunStatus: () => effects.push('status-cleared'),
    setActiveRun: () => effects.push('run-cleared'),
    setConnectionInterrupted: () => effects.push('connection-cleared'),
    touchDialog: () => effects.push('dialog-finished'),
    appendMessage: () => effects.push('message-added'),
    message: { warning: (value) => effects.push(['warning', value]), error: (value) => effects.push(['error', value]), info: () => {} },
    fetch: async () => ({ ok: true, json: async () => ({ code: 0, data: { status: 'running' } }) }),
  };
  const code = ts.transpileModule(`const handleCancel = ${cancelHandler.getText(cityBrainTree)};`, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  const createHandler = new Function(...Object.keys(bindings), `${code}\nreturn handleCancel;`);
  await createHandler(...Object.values(bindings))();
  assert(!effects.includes('closed'), 'Unconfirmed running status must retain the task stream');
  assert(!effects.includes('run-cleared'), 'Unconfirmed cancellation must retain active run');
  assert(effects.some((effect) => effect[0] === 'warning' && effect[1].includes('取消尚未确认')));
  effects.length = 0;
  bindings.fetch = async () => ({ ok: false, status: 502, json: async () => { throw new SyntaxError('Unexpected token <'); } });
  await createHandler(...Object.values(bindings))();
  assert(!effects.includes('closed'));
  assert(effects.some((effect) => effect[0] === 'error' && effect[1] === '任务服务暂不可用，取消尚未确认，请重试'));
}

const chatHooksSource = fs.readFileSync(path.join(root, 'src/hooks/chat-hooks.ts'), 'utf8');
const chatHooksTree = ts.createSourceFile('chat-hooks.ts', chatHooksSource, ts.ScriptTarget.Latest, true);
const dialogHookStatement = chatHooksTree.statements.find((node) =>
  ts.isVariableStatement(node) && node.declarationList.declarations.some((declaration) => declaration.name.getText(chatHooksTree) === 'useFetchNextDialogList'));
assert(dialogHookStatement, 'Use the actual shared sidebar hook');

async function verifyDialogRoute() {
  let query;
  const navigations = [];
  const hookBindings = {
    useClickDialogCard: () => ({}),
    useGetChatSearchParams: () => ({ dialogId: 'citybrain-synthetic' }),
    useQuery: (options) => { query = options.queryFn; return { data: [] }; },
    chatService: { listDialog: async () => ({ data: { code: 0, data: { dialogs: [{ dialog_id: 'citybrain-synthetic' }] } } }) },
    history: { push: (url) => navigations.push(url) },
    window: { location: { pathname: '/city-brain', search: '?dialogId=citybrain-synthetic' } },
    setTimeout: (callback) => callback(),
  };
  const code = ts.transpileModule(dialogHookStatement.getText(chatHooksTree).replace(/^export\s+/, ''), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  const createHook = new Function(...Object.keys(hookBindings), `${code}\nreturn useFetchNextDialogList;`);
  const useFetchNextDialogList = createHook(...Object.values(hookBindings));
  useFetchNextDialogList();
  await query();
  assert.deepEqual(navigations, [], 'Sidebar list fetch must retain the CityBrain route');
  hookBindings.window.location.pathname = '/chat';
  await query();
  assert.deepEqual(navigations, ['/chat?dialogId=citybrain-synthetic'], 'Chat keeps its existing selected-dialog behavior');
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

function streamResponse(events, broken = false) {
  let sent = false;
  return {
    ok: true,
    body: { getReader: () => ({
      async read() {
        if (!sent) {
          sent = true;
          return { value: new TextEncoder().encode(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join('')), done: false };
        }
        if (broken) throw new TypeError('synthetic disconnected stream');
        return { done: true };
      },
      releaseLock() {},
    }) },
  };
}

async function verifyResume() {
  const timers = [];
  const originalTimeout = global.setTimeout;
  const originalClear = global.clearTimeout;
  global.setTimeout = (callback) => { const timer = { callback }; timers.push(timer); return timer; };
  global.clearTimeout = (timer) => { timer.cancelled = true; };
  const nextRetry = async () => {
    const timer = timers.shift();
    assert(timer && !timer.cancelled, 'Expected a pending resume retry');
    timer.callback();
    await flush();
  };
  const runEvent = { type: '系统', status: 'running', metadata: { thread_id: 'thread-a', run_id: 'run-a', source_event: 'status' } };
  const segment = { type: 'agent', role: '执行智能体', content: 'synthetic result', metadata: { source_event: 'final_answer' } };
  const calls = [];
  const frames = [];
  const client = new SseClient('/query/deerflow?conversation_id=dialog-a&query=synthetic', { Authorization: 'test-session' });
  try {
    global.fetch = async (url) => {
      calls.push(url);
      return calls.length === 1 ? streamResponse([runEvent, segment], true) : streamResponse([runEvent, segment, { type: '[DONE]', finished: true }]);
    };
    client.connect((raw) => frames.push(JSON.parse(raw)), assert.fail);
    await flush();
    assert.deepEqual(client.activeRun, { thread_id: 'thread-a', run_id: 'run-a' });
    await nextRetry();
    assert.equal(calls.length, 2);
    const resume = new URL(calls[1], 'https://platform.example');
    assert.equal(resume.pathname, '/query/deerflow/resume');
    assert.equal(resume.searchParams.get('run_id'), 'run-a');
    assert.equal(resume.searchParams.get('conversation_id'), 'dialog-a');
    assert.equal(frames.filter((frame) => frame.metadata?.source_event === 'final_answer').length, 1, 'Replay must not duplicate segments');
    assert.equal(timers.length, 0, 'DONE must not reconnect');
    client.resume();
    await flush();
    assert.equal(calls.length, 2, 'Completed runs must not resume again');
    client.close();

    calls.length = 0;
    let failure;
    global.fetch = async (url) => { calls.push(url); return streamResponse([], true); };
    client.connect(() => {}, (error) => { failure = error; });
    await flush();
    assert(failure, 'Disconnect before run ID must be reported');
    assert.equal(calls.length, 1, 'Unknown run must never resubmit the query');
    assert.equal(timers.length, 0);
    client.close();

    calls.length = 0;
    failure = undefined;
    global.fetch = async (url) => { calls.push(url); return streamResponse([runEvent], true); };
    client.connect(() => {}, (error) => { failure = error; });
    await flush();
    for (let index = 0; index < 3; index++) await nextRetry();
    assert.equal(calls.length, 4, 'Only three resume attempts are allowed');
    assert(calls.slice(1).every((url) => url.startsWith('/query/deerflow/resume?')));
    assert(failure, 'Exhausted retries must report failure');
    global.fetch = async (url) => {
      calls.push(url);
      return streamResponse([{ type: '[DONE]', finished: true }]);
    };
    client.resume();
    await flush();
    assert.equal(calls.length, 5);
    assert(calls[4].startsWith('/query/deerflow/resume?'), 'Manual recovery must retain the accepted run');
    client.close();

    calls.length = 0;
    global.fetch = async (url) => { calls.push(url); return streamResponse([runEvent], true); };
    client.connect(() => {}, assert.fail);
    await flush();
    notifyAuthorizationChanged();
    const pending = timers.shift();
    assert(pending.cancelled, 'Account changes must cancel pending retries');
    assert.equal(calls.length, 1);

    for (const status of [401, 403, 404, 409, 429]) {
      calls.length = 0;
      failure = undefined;
      global.fetch = async (url) => {
        calls.push(url);
        return calls.length === 1 ? streamResponse([runEvent], true) : { ok: false, status };
      };
      client.connect(() => {}, (error) => { failure = error; });
      await flush();
      await nextRetry();
      assert(failure, `HTTP ${status} must terminate retries`);
      assert.equal(timers.length, 0);
      client.close();
    }

    failure = undefined;
    global.fetch = async () => streamResponse([runEvent, { type: 'error', code: 500 }, { type: '[DONE]', finished: false }]);
    const terminal = [];
    client.connect((raw) => terminal.push(JSON.parse(raw)), (error) => { failure = error; });
    await flush();
    assert.equal(terminal.at(-1).type, '[DONE]', 'Error must retain the terminal frame');
    assert.equal(timers.length, 0);
    assert.equal(failure, undefined);
  } finally {
    client.close();
    global.setTimeout = originalTimeout;
    global.clearTimeout = originalClear;
  }
}

async function main() {
  await verifyLoginRules();
  await verifyUnconfirmedCancellation();
  for (const status of ['pending', 'running', undefined, 'unknown']) assert.equal(isTerminalTaskStatus(status), false);
  for (const status of ['interrupted', 'cancelled', 'success', 'error', 'timeout']) assert.equal(isTerminalTaskStatus(status), true);
  await verifyDialogRoute();
  const warning = normalizeComplianceResult({ gate: 'InputGate', scene: '_unknown', status: 'handled', actions: ['warn', 'manual_review'], audit_ref: 'audit-synthetic', scan_complete: true });
  assert.equal(warning.final_disposition, 'manual_review');
  assert.equal(warning.audit_ref, 'audit-synthetic');
  assert.equal(warning.scan_complete, true);
  const incomplete = normalizeComplianceResult({ gate: 'InputGate', actions: ['refuse'], scan_complete: false });
  assert.equal(incomplete.final_disposition, 'refuse');
  assert.equal(incomplete.scan_complete, false);
  assert.equal(normalizeComplianceResult({ gate: 'unknown' }), null);

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
  await verifyResume();
  console.log('Multi-user client checks passed: session events, download token boundary, run-only resume, replay deduplication, bounded retries, terminal errors.');
}

main().catch((error) => { console.error(error); process.exitCode = 1; }).finally(() => {
  fs.rmSync(temporary, { recursive: true, force: true });
});
