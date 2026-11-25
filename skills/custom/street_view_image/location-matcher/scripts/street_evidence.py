"""Export bounded candidate images into the current thread, then finalize evidence."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

from PIL import Image


def outputs_root():
    return Path(os.environ.get('STREET_OUTPUT_ROOT', '/mnt/user-data/outputs')).resolve()


def output_path(value):
    path = Path(value).resolve()
    if not path.is_relative_to(outputs_root()):
        raise ValueError('Street artifacts must stay in current thread outputs')
    return path


def resolve_image(source):
    nas = Path('/mnt/nas/streetview_meta')
    local = Path(os.environ.get('STREET_LOCAL_IMAGE_ROOT', '/mnt/datasets/street/streetview_meta')).resolve()
    roots = [local]
    try:
        roots.append(nas.resolve())
    except OSError:
        pass
    requested = Path(source)
    choices = [requested]
    if requested.is_relative_to(nas):
        choices.append(local / requested.relative_to(nas))
    for candidate in choices:
        try:
            actual = candidate.resolve()
            available = any(actual.is_relative_to(root) for root in roots) and actual.is_file()
        except OSError:
            continue
        if available:
            if actual.suffix.lower() not in {'.png', '.jpg', '.jpeg', '.webp'} or actual.stat().st_size > 32 * 1024 * 1024:
                raise ValueError('Unsupported or oversized candidate image')
            with Image.open(actual) as image:
                image.verify()
            return actual
    raise ValueError('Candidate original unavailable in NAS or local street dataset')


def write_json(path, data):
    temporary = path.with_suffix('.part')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def export_candidates(hits, query_info, destination):
    folder = output_path(Path(destination) / uuid4().hex[:12])
    folder.mkdir(parents=True, exist_ok=False)
    candidates = []
    for rank, hit in enumerate(hits, 1):
        source = hit.get('_source', {})
        metadata = source.get('metadata') or {}
        row = {'rank': rank, 'id': source.get('id') or hit.get('_id'), 'es_score': hit.get('_score'),
               'source_path': source.get('source_path'), 'latitude': metadata.get('latitude'),
               'longitude': metadata.get('longitude'), 'image_path': None}
        try:
            original = resolve_image(row['source_path'] or '')
            target = folder / ('candidate_' + str(rank) + original.suffix.lower())
            shutil.copyfile(original, target)
            row.update(image_path=str(target), sha256=hashlib.sha256(target.read_bytes()).hexdigest())
        except (OSError, ValueError) as error:
            row['image_error'] = str(error)
        candidates.append(row)
    manifest_path = folder / 'retrieval.json'
    manifest = {'skill': 'location-matcher', 'schema_version': 1,
                'status': 'candidates_ready' if candidates else 'no_candidates',
                'manifest_path': str(manifest_path), 'query_info': query_info,
                'candidates': candidates, 'candidate_images': [c['image_path'] for c in candidates if c['image_path']],
                'review_required': bool(candidates)}
    write_json(manifest_path, manifest)
    return manifest


def finalize(manifest_path, matched_id, reason):
    source = output_path(manifest_path)
    manifest = json.loads(source.read_text())
    if manifest.get('skill') != 'location-matcher' or manifest.get('schema_version') != 1:
        raise ValueError('Expected a street retrieval manifest')
    if not reason.strip():
        raise ValueError('A visual matching or rejection reason is required')
    result = {'skill': 'location-matcher', 'status': 'unmatched', 'manifest_path': str(source),
              'reason': reason.strip(), 'evidence_path': str(source.parent / 'evidence.json'), 'files': []}
    if matched_id:
        matches = [c for c in manifest['candidates'] if str(c['id']) == matched_id]
        if len(matches) != 1:
            raise ValueError('Matched ID must identify one retrieved candidate')
        row = matches[0]
        image_path = output_path(row['image_path']) if row.get('image_path') else None
        if image_path is None or hashlib.sha256(image_path.read_bytes()).hexdigest() != row.get('sha256'):
            raise ValueError('Matched candidate image unavailable or changed')
        from geocoding_client import GeocodingError, coordinates, failure, reverse_geocode
        lat, lon = row['latitude'], row['longitude']
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            raise ValueError('Matched candidate lacks numeric coordinates')
        coordinates(lat, lon)
        try:
            address = reverse_geocode(lat, lon)
        except GeocodingError as error:
            address = failure(error)
        result.update(status='matched', matched_id=row['id'], latitude=lat, longitude=lon,
                      coordinate_system='WGS84', source_path=row['source_path'], image_path=str(image_path),
                      image_sha256=row['sha256'], es_score=row['es_score'], address=address.get('address'),
                      address_lookup=address, files=[str(image_path)])
    elif any(not c.get('image_path') for c in manifest['candidates']):
        result['status'] = 'unverified'
    result['files'].append(result['evidence_path'])
    write_json(Path(result['evidence_path']), result)
    return result
