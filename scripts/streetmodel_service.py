#!/usr/bin/env python3
"""Start, stop or inspect the independently deployed street embedding service."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / '.runtime/streetmodel'
SERVER = ROOT / 'services/streetmodel/server.py'
PIDFILE = RUNTIME / 'server.pid'
HOST = os.environ.get('STREET_BIND_HOST', '127.0.0.1,172.17.0.1')
PORT = int(os.environ.get('STREET_BIND_PORT', '3130'))
BASE_URL = f'http://{HOST.split(",")[0]}:{PORT}'


def own_pid():
    try:
        pid = int(PIDFILE.read_text().strip())
        command = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
        return pid if str(SERVER).encode() in command else None
    except (ValueError, OSError):
        return None


def health():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(BASE_URL + '/health', timeout=3) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'stop', 'status'])
    args = parser.parse_args()
    pid = own_pid()
    if args.action == 'stop':
        if pid:
            os.kill(pid, signal.SIGTERM)
            for _ in range(100):
                if not own_pid():
                    break
                time.sleep(.1)
            if own_pid():
                raise SystemExit('Owned service is still stopping; PID file retained')
        PIDFILE.unlink(missing_ok=True)
        print('Street model service stopped')
        return
    if args.action == 'status':
        if not pid:
            raise SystemExit('Street model service is not running')
        print(json.dumps({'pid': pid, 'base_url': BASE_URL, **health()}, ensure_ascii=False))
        return
    if pid:
        print(json.dumps({'pid': pid, 'base_url': BASE_URL, **health()}, ensure_ascii=False))
        return
    try:
        health()
    except OSError:
        pass
    else:
        raise SystemExit('A different service already responds at the requested address')
    python = RUNTIME / 'venv/bin/python'
    if not python.is_file():
        raise SystemExit('Run services/streetmodel/setup.sh first')
    RUNTIME.mkdir(parents=True, exist_ok=True)
    (RUNTIME / 'io').mkdir(exist_ok=True)
    environment = {**os.environ, 'PYTHONNOUSERSITE': '1', 'HF_HUB_OFFLINE': '1',
                   'TRANSFORMERS_OFFLINE': '1', 'TOKENIZERS_PARALLELISM': 'false', 'OMP_NUM_THREADS': '4',
                   'OPENBLAS_NUM_THREADS': '4', 'PYTHONUNBUFFERED': '1',
                   'HF_HOME': str(RUNTIME / 'huggingface'), 'TORCH_HOME': str(RUNTIME / 'torch'),
                   'XDG_CACHE_HOME': str(RUNTIME / 'cache'), 'CUDA_CACHE_PATH': str(RUNTIME / 'cuda-cache')}
    with (RUNTIME / 'server.log').open('ab') as log:
        process = subprocess.Popen([str(python), str(SERVER), '--host', HOST, '--port', str(PORT)],
                                   cwd=ROOT, env=environment, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    PIDFILE.write_text(str(process.pid) + '\n')
    for _ in range(100):
        if process.poll() is not None:
            PIDFILE.unlink(missing_ok=True)
            raise SystemExit('Street model service failed; see .runtime/streetmodel/server.log')
        try:
            state = health()
            print(json.dumps({'pid': process.pid, 'base_url': BASE_URL, **state}, ensure_ascii=False))
            return
        except OSError:
            time.sleep(.2)
    raise SystemExit('Service is starting; check status and .runtime/streetmodel/server.log')


if __name__ == '__main__':
    main()
