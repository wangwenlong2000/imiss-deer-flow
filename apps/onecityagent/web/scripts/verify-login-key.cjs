#!/usr/bin/env node
// Exercise the shipped encryption helper against real Node RSA keys, offline.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'src/utils/index.ts'), 'utf8');
const compiled = ts.transpileModule(source, {
  compilerOptions: {
    target: ts.ScriptTarget.ES2020,
    module: ts.ModuleKind.CommonJS,
    esModuleInterop: true,
  },
});
const helperModule = { exports: {} };
new Function('module', 'exports', 'require', compiled.outputText)(
  helperModule, helperModule.exports, require,
);
const { rsaPsw } = helperModule.exports;

function siteKey() {
  const keys = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
  const der = keys.publicKey.export({ type: 'spki', format: 'der' });
  return {
    privateKey: keys.privateKey,
    data: {
      public_key: keys.publicKey.export({ type: 'spki', format: 'pem' }),
      key_id: crypto.createHash('sha256').update(der).digest('hex'),
      algorithm: 'RSA-OAEP-256',
    },
  };
}

async function main() {
  const originalFetch = global.fetch;
  try {
    const first = siteKey();
    const second = siteKey();
    let current = first;
    let fetches = 0;
    global.fetch = async (url, options) => {
      fetches += 1;
      assert.equal(url, '/v1/user/public-key');
      assert.equal(options.credentials, 'same-origin');
      assert.equal(options.cache, 'no-store');
      assert(options.signal instanceof AbortSignal);
      return { ok: true, json: async () => ({ code: 0, data: current.data }) };
    };
    const password = '测试-Synthetic-Pass1!';
    const decrypt = (key, value) => crypto.privateDecrypt(
      { key, padding: crypto.constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' },
      Buffer.from(value, 'base64'),
    ).toString('utf8');
    const encrypted = await rsaPsw(password);
    assert.equal(decrypt(first.privateKey, encrypted), password);
    assert.notEqual(encrypted, await rsaPsw(password), 'OAEP must use fresh randomness');
    current = second;
    assert.equal(decrypt(second.privateKey, await rsaPsw(password)), password);
    assert.equal(fetches, 3, 'Key rotation must be observed on the next operation');

    global.fetch = async () => ({ ok: false, status: 503 });
    await assert.rejects(rsaPsw(password), /登录加密配置不可用/);
    global.fetch = async () => ({ ok: true, json: async () => ({ code: 0, data: { ...first.data, algorithm: 'RSA1_5' } }) });
    await assert.rejects(rsaPsw(password), /登录加密配置无效/);
    global.fetch = async () => { throw new Error('offline'); };
    await assert.rejects(rsaPsw(password), /offline/);

    for (const relative of [
      'src/pages/login/index.tsx',
      'src/pages/user-setting/setting-register/index.tsx',
      'src/pages/user-setting/setting-password/index.tsx',
    ]) {
      const page = fs.readFileSync(path.join(root, relative), 'utf8');
      const tree = ts.createSourceFile(relative, page, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
      let calls = 0;
      function inspect(node) {
        if (ts.isCallExpression(node) && node.expression.getText(tree) === 'rsaPsw') {
          calls += 1;
          assert(ts.isAwaitExpression(node.parent), `${relative} must await the current key`);
        }
        ts.forEachChild(node, inspect);
      }
      inspect(tree);
      assert(calls > 0, `${relative} must use the shared credential helper`);
    }
    console.log('Login key regression passed: real OAEP, Unicode, rotation, failures, all credential forms.');
  } finally {
    global.fetch = originalFetch;
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
