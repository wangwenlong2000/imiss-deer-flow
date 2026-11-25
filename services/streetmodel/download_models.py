"""Fetch pinned public model snapshots, verifying the published LFS SHA256."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib
import json
import shutil
import time
import requests

ROOT=Path(__file__).resolve().parents[2]
CONFIG=json.loads(Path(__file__).with_name('models.json').read_text())
PARTS=ROOT/'.runtime/streetmodel/download-parts'
CHUNK=32*1024*1024


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
    return h.hexdigest()


def fetch_large(url,target,size,expected,tag):
    folder=PARTS/tag.replace('/','_');folder.mkdir(parents=True,exist_ok=True)
    old=target.with_name(target.name+'.part')
    if old.exists():
        with old.open('rb') as source:
            for start in range(0,old.stat().st_size,CHUNK):
                data=source.read(CHUNK);piece=folder/f'{start:012d}.part'
                if not piece.exists() and not piece.with_suffix('.bin').exists():piece.write_bytes(data)
        old.unlink()
    response=requests.get(url,headers={'Range':'bytes=0-0'},stream=True,timeout=(20,30))
    response.raise_for_status();signed=response.url;response.close()
    def segment(start):
        end=min(start+CHUNK,size)-1;length=end-start+1
        partial=folder/f'{start:012d}.part';complete=partial.with_suffix('.bin')
        if complete.exists() and complete.stat().st_size==length:return complete
        for attempt in range(6):
            offset=partial.stat().st_size if partial.exists() else 0
            if offset==length:partial.replace(complete);return complete
            try:
                session=requests.Session();session.trust_env=False
                with session.get(signed,headers={'Range':f'bytes={start+offset}-{end}'},stream=True,timeout=(20,60)) as r:
                    r.raise_for_status()
                    if r.status_code!=206 or not r.headers.get('Content-Range','').startswith(f'bytes {start+offset}-'):
                        raise ValueError('Unexpected CDN byte range')
                    with partial.open('ab') as out:
                        for block in r.iter_content(1024*1024):out.write(block)
                if partial.stat().st_size!=length:raise ValueError('Incomplete CDN segment')
                partial.replace(complete);print('segment',tag,start,end,flush=True);return complete
            except Exception as error:
                print('segment_retry',tag,start,attempt+1,type(error).__name__,flush=True)
                if attempt==5:raise
                time.sleep(min(attempt+1,3))
    with ThreadPoolExecutor(max_workers=8) as pool:pieces=list(pool.map(segment,range(0,size,CHUNK)))
    with old.open('wb') as out:
        for piece in pieces:
            with piece.open('rb') as source:shutil.copyfileobj(source,out,8*1024*1024)
    if old.stat().st_size!=size or digest(old)!=expected:raise ValueError(f'Published hash mismatch: {tag}')
    old.replace(target)
    shutil.rmtree(folder)


def snapshot(entry):
    repo,info=entry;revision=info['sha']
    response=requests.get(f'https://huggingface.co/api/models/{repo}/revision/{revision}',params={'blobs':'true'},timeout=30)
    response.raise_for_status();metadata=response.json()
    directory=ROOT/'models/streetmodel'/repo.split('/')[-1];directory.mkdir(parents=True,exist_ok=True)
    records=[]
    for item in metadata['siblings']:
        name=item['rfilename']
        if not name.endswith(('.json','.txt','.jinja','.py','.safetensors','.pth','.md')):continue
        target=directory/name;target.parent.mkdir(parents=True,exist_ok=True)
        expected=item.get('lfs',{}).get('sha256');size=item.get('size')
        url=f'https://huggingface.co/{repo}/resolve/{revision}/{name}?download=true'
        if not target.exists() or (expected and digest(target)!=expected):
            if expected and size>8*1024*1024:
                fetch_large(url,target,size,expected,repo+'/'+name)
            else:
                for attempt in range(4):
                    try:
                        result=requests.get(url,timeout=(20,60));result.raise_for_status()
                        if size is not None and len(result.content)!=size:raise ValueError('Size mismatch')
                        target.write_bytes(result.content);break
                    except Exception:
                        if attempt==3:raise
                        time.sleep(1)
        records.append({'repo':repo,'revision':revision,'file':name,'bytes':target.stat().st_size,
                        'sha256':digest(target),'published_lfs_sha256':expected})
        print('verified',repo,name,flush=True)
    return records


if __name__=='__main__':
    with ThreadPoolExecutor(max_workers=3) as pool:groups=list(pool.map(snapshot,CONFIG.items()))
    records=[record for group in groups for record in group]
    (ROOT/'.runtime/streetmodel/downloaded-models.json').write_text(json.dumps(records,indent=2))
    print('complete',len(records),flush=True)
