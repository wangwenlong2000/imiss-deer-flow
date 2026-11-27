# User access and concurrency UI recording

The isolated walkthrough uses the original DeerFlow frontend and a temporary local bridge with three independent test identities. Requests pass through the real access and admission middleware. The recording runtime allows two active graph jobs, four total in-flight jobs and one active job per user. Its graph performs deterministic arithmetic on the uploaded CSV and waits to make queue states visible; it does not call an LLM or external tools. The test-login bridge is a recording fixture, not production SSO or an IAM deployment.

Start the frontend and demo backend in separate terminals from the repository root:

```bash
python3 scripts/serve_user_access_demo_frontend.py --port 38861 --backend-url http://127.0.0.1:38860
backend/.venv/bin/python scripts/serve_user_access_demo.py --frontend-url http://127.0.0.1:38861 --port 38860 --delay 45
```

Record with the local Chromium/FFmpeg browser image. Mount the repository read/write at `/workspace` and the output directory at `/recordings`, then run:

```bash
node /workspace/scripts/record_user_access_demo.mjs --base-url http://127.0.0.1:38860 --output /recordings --skip-render
node /workspace/scripts/render_user_access_demo.mjs --timeline /recordings/timeline.json --output /recordings/final/user-access-concurrency-demo.mp4
```

The recorder uses separate browser contexts and real frontend actions. It checks that each user's same-named CSV produces its own result, one active task per user is enforced with HTTP 429, two users can run concurrently while another queues, artifact and conversation permissions are enforced, and data-center lists remain private. The quota scene is captioned to explain that the backend returns HTTP 429 while this frontend version still displays a pending indicator; the request and response are preserved in the timeline assertions.

Keep this output local: the timeline and screenshots contain synthetic thread identifiers and test-account labels. Stop only the isolated demo services when finished.
