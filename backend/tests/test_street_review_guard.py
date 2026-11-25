import json
from langchain_core.messages import AIMessage, ToolMessage
from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware as Guard


def result(data):
    return ToolMessage(name='bash', tool_call_id='query', content=json.dumps({'skill': 'location-matcher', **data}))


def viewed(path, success=True):
    return [AIMessage(content='', tool_calls=[{'name': 'view_image', 'args': {'image_path': path}, 'id': path}]),
            ToolMessage(name='view_image', tool_call_id=path, content='Successfully read image' if success else 'Error: unavailable')]


def retrieval(paths):
    return result({'status': 'candidates_ready', 'candidate_images': paths, 'manifest_path': '/mnt/user-data/outputs/retrieval.json'})


def test_every_candidate_requires_actual_successful_view():
    turn = [retrieval(['/a.png', '/b.png']), *viewed('/a.png')]
    assert '/b.png' in Guard._street_review_gaps(turn)[0]
    assert '/b.png' in Guard._street_review_gaps([*turn, *viewed('/b.png', False)])[0]


def test_finalization_and_real_delivery_are_both_required():
    turn = [retrieval(['/a.png']), *viewed('/a.png')]
    assert 'finalize_match.py' in Guard._street_review_gaps(turn)[0]
    finalized = result({'status': 'matched', 'manifest_path': '/mnt/user-data/outputs/retrieval.json', 'files': ['/a.png', '/evidence.json']})
    assert 'present_files' in Guard._street_review_gaps([*turn, finalized])[0]
    call = AIMessage(content='', tool_calls=[{'name': 'present_files', 'args': {'filepaths': ['/a.png', '/evidence.json']}, 'id': 'deliver'}])
    done = ToolMessage(name='present_files', tool_call_id='deliver', content='Successfully presented files')
    assert Guard._street_review_gaps([*turn, finalized, call, done]) == []
    wrong_link = '[证据](/mnt/user-data/outputs/translated-name.json)'
    assert '不能翻译' in Guard._street_review_gaps([*turn, finalized, call, done], wrong_link)[0]


def test_unrelated_or_failed_queries_do_not_create_review_loops():
    assert Guard._street_review_gaps([result({'status': 'failed'})]) == []
    assert Guard._street_review_gaps([ToolMessage(name='bash', tool_call_id='x', content='not json')]) == []


def test_no_candidates_still_needs_evidence_and_old_manifest_does_not_satisfy_it():
    query = result({'status': 'no_candidates', 'candidate_images': [], 'manifest_path': '/new.json'})
    stale = result({'status': 'unmatched', 'manifest_path': '/old.json', 'files': ['/old-evidence.json']})
    assert 'finalize_match.py' in Guard._street_review_gaps([query, stale])[0]
