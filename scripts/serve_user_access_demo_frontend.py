"""Run an isolated copy of the real frontend for the access demonstration.

No .env files, production data or existing .next caches are copied. Only the
temporary copy replaces the remote Google font with a local system font.
"""

import argparse
import os
import re
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=38861)
    parser.add_argument("--backend-url", default="http://127.0.0.1:38860")
    args = parser.parse_args()
    frontend = Path(__file__).resolve().parents[1] / "frontend"
    cli = frontend / "node_modules/next/dist/bin/next"
    if not cli.is_file():
        raise SystemExit("Install the frontend dependencies before running the demo")
    with socket.socket() as check:
        check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        check.bind(("127.0.0.1", args.port))
    with tempfile.TemporaryDirectory(prefix="deerflow-demo-frontend-") as temporary:
        root = Path(temporary)
        shutil.copytree(frontend / "src", root / "src")
        for name in ("package.json", "tsconfig.json", "next.config.js", "postcss.config.js"):
            shutil.copy2(frontend / name, root / name)
        (root / "node_modules").symlink_to(frontend / "node_modules", target_is_directory=True)
        (root / "public").symlink_to(frontend / "public", target_is_directory=True)
        layout = root / "src/app/layout.tsx"
        source = layout.read_text()
        source = source.replace('import { Geist } from "next/font/google";\n', "")
        source = re.sub(r"const geist = Geist\(\{.*?\}\);", "", source, flags=re.DOTALL)
        source = source.replace("className={geist.variable}", 'className="demo-local-font"')
        layout.write_text(source)
        # Use a clean environment rather than inheriting provider credentials.
        environment = {
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "HOME": str(root),
            "NODE_ENV": "development",
            "SKIP_ENV_VALIDATION": "1",
            "NEXT_TELEMETRY_DISABLED": "1",
            "DEERFLOW_GATEWAY_URL": args.backend_url,
            "DEERFLOW_LANGGRAPH_URL": args.backend_url + "/api/langgraph",
        }
        process = subprocess.Popen(
            [shutil.which("node") or "node", str(cli), "dev", "--webpack", "-H", "127.0.0.1", "-p", str(args.port)],
            cwd=root,
            env=environment,
        )
        print(f"Isolated frontend: http://127.0.0.1:{args.port}", flush=True)
        try:
            process.wait()
        except KeyboardInterrupt:
            pass
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
