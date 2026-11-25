"""Contract regressions; real-model verification is recorded separately."""
import base64
import importlib.util
import io
import json
from pathlib import Path
import sys

from fastapi.testclient import TestClient
from PIL import Image
import pytest

SERVER=Path(__file__).resolve().parents[1]/'server.py'
spec=importlib.util.spec_from_file_location('street_rest_test',SERVER)
server=importlib.util.module_from_spec(spec);sys.modules[spec.name]=server;spec.loader.exec_module(server)
client=TestClient(server.app)


def image_item():
    buffer=io.BytesIO();Image.new('RGB',(8,8),'red').save(buffer,format='PNG')
    return {'type':'image_base64','data':'data:image/png;base64,'+base64.b64encode(buffer.getvalue()).decode()}


@pytest.mark.parametrize('item',[{'type':'image_base64','data':'broken!!!'}, {'type':'image_base64'}, {'type':'text','content':''}])
def test_bad_inputs_fail_before_model_loading(item):
    assert client.post('/embed',json={'items':[item]}).status_code==400
    assert not server.registry.loaded


def test_inline_image_reaches_the_configured_embedding_model(monkeypatch):
    calls=[]
    def embed(name,records,instruction,batch_size):
        calls.append((name,records,instruction,batch_size));return [[1,0]]
    monkeypatch.setattr(server.registry,'embed',embed)
    response=client.post('/embed',json={'model_name':server.IMAGE_MODEL,'items':[image_item()],'instruction':'test','batch_size':2})
    assert response.status_code==200 and response.json()['embeddings']==[[1,0]]
    name,records,instruction,batch_size=calls[0]
    assert name==server.IMAGE_MODEL and records[0]['image'].mode=='RGB'
    assert instruction=='test' and batch_size==2


def test_utf8_document_and_legacy_local_image_paths(monkeypatch,tmp_path):
    monkeypatch.setattr(server,'DATA_ROOTS',[tmp_path])
    text=tmp_path/'source.txt';text.write_text('街景描述')
    assert server.parse_item(server.Item(type='document',uri=str(text)))=={'text':'街景描述'}
    picture=tmp_path/'image.png';Image.new('RGB',(8,8)).save(picture)
    assert server.inputs(None,[str(picture)])[0]['image'].size==(8,8)


def test_paths_cannot_read_outside_dataset_roots(tmp_path,monkeypatch):
    monkeypatch.setattr(server,'DATA_ROOTS',[tmp_path])
    response=client.post('/embed',json={'items':[{'type':'image','uri':'/etc/passwd'}]})
    assert response.status_code==403


def test_batch_writes_complete_records_and_metadata(monkeypatch,tmp_path):
    monkeypatch.setattr(server,'RUNTIME',tmp_path)
    monkeypatch.setattr(server,'DATA_ROOTS',[tmp_path])
    picture=tmp_path/'sample.png';Image.new('RGB',(8,8)).save(picture)
    source=tmp_path/'source.jsonl';source.write_text('\n'.join(json.dumps({'id':i,'image_path':str(picture)}) for i in range(3)))
    monkeypatch.setattr(server.registry,'embed',lambda name,records,instruction,batch_size:[[1,0] for _ in records])
    response=client.post('/batch',json={'jsonl_path':str(source),'model_name':server.IMAGE_MODEL,'batch_size':2})
    assert response.status_code==200
    rows=[json.loads(line) for line in Path(response.json()['output_path']).read_text().splitlines()]
    assert response.json()['processed_count']==3 and rows[0]['meta']['model_name']==server.IMAGE_MODEL
    assert [row['id'] for row in rows[1:]]==[0,1,2]


def test_bad_batch_does_not_publish_partial_results(monkeypatch,tmp_path):
    monkeypatch.setattr(server,'RUNTIME',tmp_path)
    monkeypatch.setattr(server,'DATA_ROOTS',[tmp_path])
    picture=tmp_path/'sample.png';Image.new('RGB',(8,8)).save(picture)
    source=tmp_path/'bad.jsonl';source.write_text(json.dumps({'id':'ok','image_path':str(picture)})+'\n{invalid')
    monkeypatch.setattr(server.registry,'embed',lambda name,records,instruction,batch_size:[[1,0] for _ in records])
    response=client.post('/batch',json={'jsonl_path':str(source),'model_name':server.IMAGE_MODEL,'batch_size':1})
    assert response.status_code==400
    assert not list((tmp_path/'io/batch').glob('*'))


def test_empty_requests_and_invalid_batch_sizes_are_rejected():
    assert client.post('/embed',json={'items':[]}).status_code==400
    assert client.post('/embed',json={'items':[image_item()],'batch_size':0}).status_code==422
    assert client.get('/health').json()['status']=='ok'
    assert {row['name'] for row in client.get('/models').json()['models']}=={server.IMAGE_MODEL,server.EMBED_MODEL,server.RERANK_MODEL}


def test_unsupported_models_are_rejected_before_loading_or_writing():
    assert client.post('/embed',json={'model_name':'unknown','items':[image_item()]}).status_code==400
    assert client.post('/rerank',json={'model_name':'unknown'}).status_code==400
    assert client.post('/batch',json={'model_name':'../../escape','jsonl_path':'/etc/passwd'}).status_code==400
