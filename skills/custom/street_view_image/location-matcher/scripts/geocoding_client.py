"""WGS84 lookup via Photon, with shared caching and bounded requests."""
from __future__ import annotations
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import time
from urllib.parse import urlsplit
import requests

class GeocodingError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)

def coordinates(latitude, longitude):
    if not (math.isfinite(latitude) and math.isfinite(longitude) and -90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise GeocodingError('INVALID_COORDINATES', 'Expected finite WGS84 latitude/longitude')

def _query(endpoint, params):
    try:
        return _cached_query(endpoint, params)
    except OSError as error:
        raise GeocodingError('GEOCODING_CACHE_UNAVAILABLE', 'Address cache unavailable; retain verified coordinates') from error

def _cached_query(endpoint, params):
    base = os.environ.get('STREET_GEOCODING_BASE_URL', 'https://photon.komoot.io').rstrip('/')
    parsed = urlsplit(base)
    if parsed.scheme not in {'https', 'http'} or not parsed.hostname or parsed.username or parsed.password or parsed.query:
        raise GeocodingError('GEOCODING_CONFIG_INVALID', 'Invalid geocoding base URL')
    if parsed.scheme == 'http' and parsed.hostname not in {'127.0.0.1', 'localhost'}:
        raise GeocodingError('GEOCODING_CONFIG_INVALID', 'Remote geocoding requires HTTPS')
    shared = Path(os.environ.get('STREET_GEOCODING_CACHE_DIR', '/tmp/deerflow-street-geocoding'))
    shared.mkdir(parents=True, exist_ok=True)
    thread_workspace = Path('/mnt/user-data/workspace')
    cache = thread_workspace / '.street-geocoding-cache' if thread_workspace.is_dir() else shared
    cache.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(json.dumps([base, endpoint, params], sort_keys=True).encode()).hexdigest()
    target = cache / (key + '.json')
    with (shared / 'request.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if target.exists() and time.time() - target.stat().st_mtime < 86400:
            try:
                data = json.loads(target.read_text())
                if isinstance(data, dict) and isinstance(data.get('features'), list):
                    return data
            except ValueError:
                pass
            target.unlink()
        stamp = shared / 'last-request'
        if stamp.exists():
            time.sleep(max(0, min(1.1, 1.1 - (time.time() - stamp.stat().st_mtime))))
        stamp.touch()
        try:
            response = requests.get(base + endpoint, params=params, timeout=(4, 12),
                                    headers={'User-Agent': 'DeerFlow-Street-Geocoding/1.0', 'Accept': 'application/json'})
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as error:
            raise GeocodingError('GEOCODING_UNAVAILABLE', 'Address service unavailable; retain verified coordinates') from error
        if not isinstance(data, dict) or not isinstance(data.get('features'), list) or any(not isinstance(f, dict) or not isinstance(f.get('properties'), dict) for f in data['features']):
            raise GeocodingError('GEOCODING_INVALID_RESPONSE', 'Address service returned invalid data')
        temporary = target.with_suffix('.part')
        temporary.write_text(json.dumps(data, ensure_ascii=False))
        temporary.replace(target)
        return data

def _result(feature):
    try:
        lon, lat = feature['geometry']['coordinates']
        coordinates(lat, lon)
        parts = feature['properties']
        fields = ['country', 'state', 'city', 'district', 'locality', 'street', 'housenumber', 'name']
        label = ' / '.join(dict.fromkeys(str(parts[key]) for key in fields if parts.get(key)))
        if not label:
            raise ValueError('Missing address label')
    except (KeyError, TypeError, ValueError) as error:
        raise GeocodingError('GEOCODING_INVALID_RESPONSE', 'Invalid address result') from error
    return {'status': 'success', 'address': label, 'latitude': lat, 'longitude': lon,
            'coordinate_system': 'WGS84', 'provider': 'photon', 'address_precision': parts.get('type', 'unknown'),
            'address_components': parts, 'attribution': '© OpenStreetMap contributors (ODbL)',
            'attribution_url': 'https://www.openstreetmap.org/copyright'}

def geocode(address, countrycode=None):
    if not address.strip():
        raise GeocodingError('INVALID_ADDRESS', 'Address must not be empty')
    params = {'q': address.strip(), 'limit': 2}
    if countrycode:
        if len(countrycode) != 2 or not countrycode.isalpha():
            raise GeocodingError('INVALID_COUNTRY', 'Use a two-letter country code')
        params['countrycode'] = countrycode.upper()
    features = _query('/api/', params)['features']
    if not features:
        raise GeocodingError('GEOCODING_NO_RESULT', 'No address result in selected coverage')
    result = _result(features[0])
    if len(features) > 1 and features[0]['properties'].get('countrycode') != features[1]['properties'].get('countrycode'):
        raise GeocodingError('GEOCODING_AMBIGUOUS', 'Provide country or a fuller address')
    if countrycode and result['address_components'].get('countrycode', '').upper() != countrycode.upper():
        raise GeocodingError('GEOCODING_COVERAGE_MISMATCH', 'Result outside requested country')
    return result

def reverse_geocode(latitude, longitude):
    coordinates(latitude, longitude)
    features = _query('/reverse', {'lat': latitude, 'lon': longitude, 'radius': 0.2, 'limit': 1})['features']
    if not features:
        raise GeocodingError('GEOCODING_NO_RESULT', 'No mapped address within 200 metres')
    result = _result(features[0])
    result['address_location'] = {'latitude': result['latitude'], 'longitude': result['longitude']}
    lat1, lat2 = math.radians(latitude), math.radians(result['latitude'])
    angle = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(math.radians(result['longitude'] - longitude) / 2) ** 2
    distance = 12742000 * math.asin(min(1, math.sqrt(angle)))
    if distance > 250:
        raise GeocodingError('GEOCODING_COVERAGE_MISMATCH', 'Returned address is outside lookup radius')
    result['address_distance_metres'] = round(distance, 1)
    result.update(latitude=latitude, longitude=longitude, address_relation='nearby_mapped_feature')
    return result

def failure(error):
    return {'status': 'failed', 'error_code': error.code, 'message': str(error), 'address': None}
