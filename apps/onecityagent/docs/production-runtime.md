# OneCity core production runtime

The deployable core runs the API and Coordinator factories independently. The
supported release target is Linux x86_64 with Python 3.13. Optional legacy A2A,
browser and MCP agents are outside this image and retain their existing full
development dependency list.

## Dependencies

`requirements-core.txt` is the source for direct core runtime requirements.
`requirements-core.lock` fixes the complete transitive dependency graph and
artifact SHA-256 hashes for the release target. The Dockerfile installs only this
lock with `pip --require-hashes` and runs `pip check`; it does not resolve floating
runtime dependencies during an installation.

To regenerate deliberately, install `uv==0.9.9` into an isolated tooling
environment, put its binary on `PATH`, and run:

```sh
sh apps/onecityagent/scripts/lock_core.sh
```

Review the resulting versions and hashes, rebuild the image, and run the real
factory tests on Python 3.13 before releasing. Resolving for Python 3.13 is not
evidence that the application has executed on Python 3.13. The Docker build
argument `PYTHON_IMAGE` permits a release manifest to select the reviewed
Python 3.13 base image by digest; a Python 3.12 smoke image is development-only.

## Site login key

`GET /v1/user/public-key` is public and returns only the public half of the
configured site key:

```json
{
  "code": 0,
  "data": {
    "public_key": "-----BEGIN PUBLIC KEY-----...",
    "key_id": "SHA-256 of the SubjectPublicKeyInfo DER, lowercase hexadecimal",
    "algorithm": "RSA-OAEP-256"
  },
  "message": "success"
}
```

The existing login, registration and password-change payloads continue to use
base64-encoded RSA-OAEP ciphertext with SHA-256 for both the OAEP hash and MGF1.
The browser retrieves the current key from the same origin before every
credential operation. It never falls back to a compiled key or plaintext.
The key response has `Cache-Control: no-store`; missing, invalid or sub-2048-bit
private keys produce a sanitized HTTP 503. Keep HTTPS enabled at the public
entry point; password encryption does not replace transport authentication.

`ONECITY_RSA_PRIVATE_KEY_PATH` and
`ONECITY_RSA_PRIVATE_KEY_PASSPHRASE` identify the configured key. The deployment
entrypoint can read the passphrase through its corresponding `_FILE` setting.
During migration import the existing site key; do not silently rotate it.

Regression commands:

```sh
cd apps/onecityagent
python -m pytest tests -q
cd web
npm run test:login-key
node scripts/verify-multi-user-client.cjs
```

The new Python tests use temporary keys and SQLite catalogues. The browser-helper
test uses real RSA encryption and decryption locally; neither test contacts a
running site or needs a real account.

Production database connections use `autoconnect=False`. Authentication and
history regressions also disable automatic SQLite connections so an implicit
reconnect cannot hide request lifecycle defects. Database contexts preserve an
already open connection owned by a request or outer transaction, and close only
a connection they opened. A successful login persists its new token before
establishing the session. API diagnostics record paths and exception types only.
