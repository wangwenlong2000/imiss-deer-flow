"""Exercise the live service to completion, keeping graph/tool/artifact evidence.

No production code or scene/model configuration is changed. Conversation turns
are sequential; different sessions have at most two concurrent runs.
"""
import argparse
import concurrent.futures
import hashlib
import json
import mimetypes
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUT_ROOT = ROOT
RECURSION_LIMIT = 100
ORIGIN = 'http://127.0.0.1:3328'
BASE = '/api/langgraph'
LOCK = threading.Lock()


def log(**record):
    with LOCK:
        print(json.dumps(record, ensure_ascii=False), flush=True)


def request(path, body=None, timeout=60):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(ORIGIN + path, data=data, headers={'Content-Type': 'application/json'})
    return urllib.request.urlopen(req, timeout=timeout)


def read_json(path, body=None):
    with request(path, body) as response:
        return json.load(response)


def save(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + '\n')


def upload(thread_id, filename):
    raw = (INPUT_ROOT / 'fixtures' / filename).read_bytes()
    boundary = 'full-flow-' + uuid.uuid4().hex
    content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="{filename}"\r\n'
              f'Content-Type: {content_type}\r\n\r\n').encode()
    req = urllib.request.Request(ORIGIN + f'/api/threads/{thread_id}/uploads',
                                 data=prefix + raw + f'\r\n--{boundary}--\r\n'.encode(),
                                 headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
    with urllib.request.urlopen(req, timeout=90) as response:
        result = json.load(response)
    if not result.get('success'):
        raise RuntimeError('Upload failed: ' + str(result.get('message')))
    return result


def text_content(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return '\n'.join(x.get('text', '') for x in content if isinstance(x, dict))
    return str(content)


def download_outputs(thread_id, stem):
    listing = read_json(f'/api/threads/{thread_id}/workspace-files')
    files = []
    destination = ROOT / 'downloads' / stem
    destination.mkdir(parents=True, exist_ok=True)
    for item in listing.get('files', []):
        result = dict(item)
        try:
            if item['size_bytes'] > 10 * 1024 * 1024:
                result['download_skipped'] = '10MB evaluation limit'
            else:
                url_path = '/api/threads/' + thread_id + '/artifacts/' + urllib.parse.quote(item['virtual_path'].lstrip('/'), safe='/') + '?download=true'
                with request(url_path) as response:
                    data = response.read(10 * 1024 * 1024 + 1)
                    result['http_status'] = response.status
                local = destination / item['relative_path']
                local.parent.mkdir(parents=True, exist_ok=True)
                local.write_bytes(data)
                result.update(local_path=str(local.relative_to(ROOT)), downloaded_bytes=len(data),
                              sha256=hashlib.sha256(data).hexdigest(), size_matches=len(data) == item['size_bytes'])
        except Exception as exc:
            result['download_error'] = type(exc).__name__ + ': ' + str(exc)[:400]
        files.append(result)
    return files


def one_turn(case, index, turn, tid, previous):
    start = time.monotonic()
    stem = case['id'] + '-turn-' + str(index + 1)
    row = {'case_id': case['id'], 'turn': index + 1, 'thread_id': tid,
           'query': turn['query'], 'expected_scenes': turn['expected_scenes'], 'expected': turn['expected']}
    log(event='turn_start', case_id=case['id'], turn=index + 1, thread_id=tid)
    message = {'role': 'human', 'content': turn['query'], 'id': str(uuid.uuid4())}
    if turn.get('upload'):
        us = time.monotonic()
        result = upload(tid, turn['upload'])
        row.update(upload_seconds=time.monotonic() - us, upload=result)
        message['additional_kwargs'] = {'files': [dict(f, status='uploaded') for f in result['files']]}
    payload = {'assistant_id': 'lead_agent', 'input': {'messages': [message]},
               'context': {'thread_id': tid, 'model_name': 'qwen3.6-35b-a3b', 'thinking_enabled': False,
                           'subagent_enabled': False},
               'config': {'recursion_limit': RECURSION_LIMIT},
               'stream_mode': ['updates', 'values', 'messages-tuple']}
    save(ROOT / (stem + '.request.json'), payload)
    stream_start = time.monotonic()
    nodes, calls, state = [], {}, {}
    errors = []
    with (ROOT / (stem + '.events.jsonl')).open('w') as trace:
        with request(BASE + f'/threads/{tid}/runs/stream', payload) as response:
            event, lines = '', []
            for raw in response:
                if time.monotonic() - stream_start > 480:
                    errors.append('Evaluation deadline exceeded (480 seconds)')
                    if row.get('run_id'):
                        read_json(BASE + f'/threads/{tid}/runs/{row["run_id"]}/cancel', {'action': 'interrupt'})
                    break
                line = raw.decode().rstrip('\r\n')
                if line.startswith('event:'):
                    event = line[6:].strip()
                elif line.startswith('data:'):
                    lines.append(line[5:].lstrip())
                elif not line and lines:
                    elapsed = time.monotonic() - stream_start
                    data = json.loads('\n'.join(lines))
                    lines = []
                    trace.write(json.dumps({'event': event, 'seconds': elapsed, 'data': data}, ensure_ascii=False) + '\n')
                    trace.flush()
                    if event == 'metadata':
                        row['run_id'] = data.get('run_id')
                    elif event == 'error':
                        errors.append(data)
                    elif event == 'values' and isinstance(data, dict):
                        state = data
                    elif event == 'updates' and isinstance(data, dict):
                        for node in data:
                            nodes.append({'node': node, 'seconds': elapsed})
                            log(event='node', case_id=case['id'], turn=index + 1, node=node, seconds=round(elapsed, 2))
                    elif event.startswith('messages') and isinstance(data, list) and len(data) == 2:
                        msg, meta = data
                        if msg.get('id') and meta.get('ls_model_name'):
                            call = calls.setdefault(msg['id'], {'model': meta['ls_model_name'], 'node': meta.get('langgraph_node'),
                                                               'tags': meta.get('tags', []), 'first_seconds': elapsed})
                            call['last_seconds'] = elapsed
                            if msg.get('usage_metadata'):
                                call['usage'] = msg['usage_metadata']
    row['stream_seconds'] = time.monotonic() - stream_start
    snap = read_json(BASE + f'/threads/{tid}/state')
    save(ROOT / (stem + '.state.json'), snap)
    state = snap.get('values') or state
    old_ids = {m.get('id') for m in previous.get('messages', [])}
    current = [m for m in state.get('messages', []) if m.get('id') not in old_ids]
    assistants = [m for m in current if m.get('type', m.get('role')) in {'ai', 'assistant'}]
    tools = [m for m in current if m.get('type', m.get('role')) == 'tool']
    requested = [dict(t, assistant_message_id=m.get('id')) for m in assistants for t in m.get('tool_calls', [])]
    answers = [m for m in assistants if not m.get('tool_calls') and text_content(m.get('content'))]
    pending = state.get('pending_action')
    row.update(next=snap.get('next'), pending_action=pending, intent=state.get('intent_context'),
               dialogue=state.get('dialogue_context'), nodes=nodes, model_calls=list(calls.values()),
               tools_requested=requested, tool_responses=tools,
               final_answer=text_content(answers[-1].get('content')) if answers else '',
               artifacts=state.get('artifacts'), errors=errors,
               graph_finished=not snap.get('next') and not errors,
               business_model_ran=any(n['node'] == 'model' for n in nodes),
               tool_node_ran=any(n['node'] == 'tools' for n in nodes))
    try:
        row['downloaded_outputs'] = download_outputs(tid, stem)
    except Exception as exc:
        row['artifact_listing_error'] = type(exc).__name__ + ': ' + str(exc)[:400]
    row['seconds_including_upload_and_download'] = time.monotonic() - start
    save(ROOT / (stem + '.result.json'), row)
    (ROOT / (stem + '.answer.md')).write_text(row['final_answer'] + '\n')
    log(event='turn_finished', case_id=case['id'], turn=index + 1,
        seconds=round(row['stream_seconds'], 2), scene=(row['intent'] or {}).get('scenes'),
        calls=len(calls), tools=[t.get('name') for t in requested], pending=bool(pending),
        output_count=len(row.get('downloaded_outputs', [])), errors=errors)
    return row, state


def session(case):
    start = time.monotonic()
    tid = read_json(BASE + '/threads', {'metadata': {'purpose': 'full-flow-synthetic-20261022', 'test_case': case['id']}})['thread_id']
    previous, rows = {}, []
    for index, turn in enumerate(case['turns']):
        try:
            row, previous = one_turn(case, index, turn, tid, previous)
        except Exception as exc:
            row = {'case_id': case['id'], 'turn': index + 1, 'thread_id': tid,
                   'error': type(exc).__name__ + ': ' + str(exc)[:700]}
            save(ROOT / (case['id'] + '-turn-' + str(index + 1) + '.result.json'), row)
            log(event='turn_exception', **row)
        rows.append(row)
        with LOCK:
            with (ROOT / 'results.jsonl').open('a') as handle:
                handle.write(json.dumps(row, ensure_ascii=False) + '\n')
        if row.get('error') or row.get('errors') or row.get('next'):
            break
    log(event='session_finished', case_id=case['id'], seconds=round(time.monotonic() - start, 2))
    return rows


def main():
    global ROOT, RECURSION_LIMIT
    parser = argparse.ArgumentParser()
    parser.add_argument('--ids')
    parser.add_argument('--output-run')
    parser.add_argument('--recursion-limit', type=int, default=100)
    args = parser.parse_args()
    data = json.loads((INPUT_ROOT / 'cases.json').read_text())
    RECURSION_LIMIT = args.recursion_limit
    if args.output_run:
        ROOT = INPUT_ROOT / args.output_run
        ROOT.mkdir(exist_ok=False)
        (ROOT / 'downloads').mkdir()
    save(ROOT / 'run-config.json', {'recursion_limit': RECURSION_LIMIT, 'concurrent_sessions': 2,
                                   'interrupt_before': None, 'business_deadline_seconds': 480})
    if args.ids:
        data = [c for c in data if c['id'] in args.ids.split(',')]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(session, data))
    rows = [r for group in results for r in group]
    log(event='all_finished', turns=len(rows), exceptions=sum(bool(r.get('error')) for r in rows))


if __name__ == '__main__':
    main()
