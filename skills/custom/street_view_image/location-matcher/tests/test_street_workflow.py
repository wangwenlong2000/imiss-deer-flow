import importlib
import json
import os
from pathlib import Path
import sys

from PIL import Image
import pytest
import requests

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
geo = importlib.import_module('geocoding_client')
evidence = importlib.import_module('street_evidence')


def feature(lat=35.65, lon=139.7, country='JP', street='test road'):
    return {'properties': {'country': 'Japan', 'countrycode': country, 'city': 'Tokyo', 'street': street, 'type': 'street'},
            'geometry': {'coordinates': [lon, lat]}}


def test_reverse_preserves_original_coordinates_and_reports_nearby_address(monkeypatch):
    monkeypatch.setattr(geo, '_query', lambda *a: {'features': [feature()]})
    result = geo.reverse_geocode(35.651, 139.701)
    assert (result['latitude'], result['longitude']) == (35.651, 139.701)
    assert result['address_location'] == {'latitude': 35.65, 'longitude': 139.7}
    assert result['coordinate_system'] == 'WGS84'
    assert 'test road' in result['address']


@pytest.mark.parametrize('lat,lon', [(91, 0), (0, 181), (float('nan'), 0), (0, float('inf'))])
def test_invalid_coordinates_do_not_call_provider(monkeypatch, lat, lon):
    monkeypatch.setattr(geo, '_query', lambda *a: pytest.fail('Must reject before networking'))
    with pytest.raises(geo.GeocodingError, match='WGS84'):
        geo.reverse_geocode(lat, lon)


def test_empty_or_ambiguous_address_has_no_fixed_fallback(monkeypatch):
    with pytest.raises(geo.GeocodingError):
        geo.geocode(' ')
    monkeypatch.setattr(geo, '_query', lambda *a: {'features': [feature(), feature(country='US')]})
    with pytest.raises(geo.GeocodingError) as error:
        geo.geocode('Springfield')
    assert error.value.code == 'GEOCODING_AMBIGUOUS'
    assert geo.failure(error.value)['address'] is None


def test_no_coverage_and_wrong_country_are_not_success(monkeypatch):
    monkeypatch.setattr(geo, '_query', lambda *a: {'features': []})
    with pytest.raises(geo.GeocodingError):
        geo.reverse_geocode(0, 0)
    monkeypatch.setattr(geo, '_query', lambda *a: {'features': [feature(country='US')]})
    with pytest.raises(geo.GeocodingError):
        geo.geocode('Tokyo Tower', 'JP')


def test_cache_avoids_duplicate_requests_and_errors_do_not_leak_urls(monkeypatch, tmp_path):
    monkeypatch.setenv('STREET_GEOCODING_CACHE_DIR', str(tmp_path))
    calls = []
    def get(*a, **kw):
        calls.append(kw)
        return type('Response', (), {'raise_for_status': lambda self: None,
                                    'json': lambda self: {'features': [feature()]}})()
    monkeypatch.setattr(geo.requests, 'get', get)
    assert geo._query('/reverse', {'lat': 35, 'lon': 139}) == geo._query('/reverse', {'lat': 35, 'lon': 139})
    assert len(calls) == 1
    monkeypatch.setattr(geo.time, 'sleep', lambda seconds: None)
    def fail(*a, **kw):
        raise requests.ConnectionError('secret-token-in-URL')
    monkeypatch.setattr(geo.requests, 'get', fail)
    with pytest.raises(geo.GeocodingError) as error:
        geo._query('/reverse', {'lat': 36, 'lon': 139})
    assert 'secret-token' not in str(error.value)


@pytest.fixture
def exported(monkeypatch, tmp_path):
    out = tmp_path / 'outputs';out.mkdir()
    local = tmp_path / 'local';local.mkdir()
    monkeypatch.setenv('STREET_OUTPUT_ROOT', str(out))
    monkeypatch.setenv('STREET_LOCAL_IMAGE_ROOT', str(local))
    original = local / 'test.png';Image.new('RGB', (8, 8), 'green').save(original)
    hits = [{'_source': {'id': 'known', 'source_path': '/mnt/nas/streetview_meta/test.png',
                         'metadata': {'latitude': 35.65, 'longitude': 139.7}}, '_score': .99}]
    return evidence.export_candidates(hits, {}, out / 'street'), out


def test_nas_fallback_exports_real_image_and_pending_manifest(exported):
    result, out = exported
    assert result['status'] == 'candidates_ready'
    assert Path(result['candidate_images'][0]).is_relative_to(out)
    assert Path(result['manifest_path']).is_file()
    assert 'matched_id' not in result


def test_unavailable_nas_mount_does_not_prevent_local_fallback(monkeypatch, exported):
    local_source = Path(os.environ['STREET_LOCAL_IMAGE_ROOT']) / 'test.png'
    resolve = Path.resolve
    def unavailable(path, *args, **kwargs):
        if str(path).startswith('/mnt/nas/streetview_meta'):
            raise OSError(19, 'No such device')
        return resolve(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'resolve', unavailable)
    assert evidence.resolve_image('/mnt/nas/streetview_meta/test.png') == local_source


def test_reverse_rejects_a_successful_response_from_another_region(monkeypatch):
    monkeypatch.setattr(geo, '_query', lambda *a: {'features': [feature(lat=31, lon=121)]})
    with pytest.raises(geo.GeocodingError) as error:
        geo.reverse_geocode(35.65, 139.7)
    assert error.value.code == 'GEOCODING_COVERAGE_MISMATCH'


def test_final_evidence_keeps_coordinates_when_address_service_fails(monkeypatch, exported):
    manifest, _ = exported
    def fail(*args):
        raise geo.GeocodingError('GEOCODING_UNAVAILABLE', 'network unavailable')
    monkeypatch.setattr(geo, 'reverse_geocode', fail)
    result = evidence.finalize(manifest['manifest_path'], 'known', 'Same building and road layout')
    assert result['status'] == 'matched' and result['latitude'] == 35.65
    assert result['address'] is None and result['address_lookup']['status'] == 'failed'
    assert all(Path(path).is_file() for path in result['files'])


def test_finalizer_rejects_unretrieved_id_and_changed_image(exported):
    manifest, _ = exported
    with pytest.raises(ValueError):
        evidence.finalize(manifest['manifest_path'], 'fabricated', 'reason')
    Path(manifest['candidate_images'][0]).write_bytes(b'changed')
    with pytest.raises(ValueError):
        evidence.finalize(manifest['manifest_path'], 'known', 'reason')


def test_paths_cannot_escape_street_roots_or_thread_outputs(exported, tmp_path):
    manifest, out = exported
    with pytest.raises(ValueError):
        evidence.output_path(out / '..' / 'escape')
    unrelated = tmp_path / 'unrelated.png';Image.new('RGB', (8, 8)).save(unrelated)
    with pytest.raises(ValueError):
        evidence.resolve_image(str(unrelated))
    with pytest.raises(ValueError):
        evidence.finalize(manifest['manifest_path'], 'known', '')


def test_empty_retrieval_and_unreadable_candidates_are_not_matches(exported):
    _, out = exported
    manifest = evidence.export_candidates([], {}, out / 'empty')
    result = evidence.finalize(manifest['manifest_path'], None, 'No candidates above original threshold')
    assert result['status'] == 'unmatched' and Path(result['evidence_path']).is_file()
    unavailable = evidence.export_candidates([{'_source': {'id': 'missing', 'source_path': '/etc/passwd'}}], {}, out / 'missing')
    assert not unavailable['candidate_images']
    assert evidence.finalize(unavailable['manifest_path'], None, 'Cannot inspect original')['status'] == 'unverified'


@pytest.mark.parametrize('flags', [['--lat', '35', '--lon', '139'], ['--latitude', '35', '--longitude', '139']])
def test_both_coordinate_cli_aliases_work(monkeypatch, capsys, flags):
    cli = importlib.import_module('reverse_geocoding')
    monkeypatch.setattr(cli, 'reverse_geocode', lambda lat, lon: {'status': 'success', 'latitude': lat, 'longitude': lon})
    monkeypatch.setattr(sys, 'argv', ['reverse_geocoding.py', *flags])
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)['latitude'] == 35
