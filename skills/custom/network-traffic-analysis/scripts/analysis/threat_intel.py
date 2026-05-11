"""
Threat Intelligence Integration Module

Provides IOC matching using:
- Bloom filter for fast IP/domain membership testing
- CIDR matching with proper bit-level operations
- Levenshtein distance for typosquatting detection
- Entropy-based DGA domain detection
- Real open-source threat feed integration

References:
- Bloom, B. H. (1970). "Space/time trade-offs in hash coding with allowable errors"
- Levenshtein, V. I. (1966). "Binary codes capable of correcting deletions, insertions, and reversals"
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class ThreatIntelResult:
    """Threat intelligence matching result."""
    entity_id: str
    entity_type: str  # ip, domain, url, hash
    matched: bool
    threat_type: str
    severity: str
    reputation_score: float
    mitre_techniques: list[str]
    campaigns: list[str]
    enrichment: dict[str, Any]
    recommended_actions: list[str]
    source: str = ""
    source_url: str = ""
    first_seen: str | None = None
    last_seen: str | None = None
    confidence: float = 0.0
    cache_age_hours: float | None = None
    coverage_mode: str = "heuristic"


@dataclass
class IndicatorRecord:
    """IOC record loaded from a local cache or refreshed public feed."""

    entity: str
    entity_type: str
    threat_type: str
    severity: str
    source: str
    source_url: str = ""
    description: str = ""
    mitre_techniques: list[str] = field(default_factory=list)
    campaigns: list[str] = field(default_factory=list)
    confidence: float = 0.75
    first_seen: str | None = None
    last_seen: str | None = None
    tags: list[str] = field(default_factory=list)
    ttl_hours: int | None = None
    cache_age_hours: float | None = None
    reputation_score: float | None = None


class BloomFilter:
    """
    Bloom filter for fast IOC membership testing.
    
    Space-efficient probabilistic data structure.
    False positives possible, false negatives impossible.
    
    Optimal parameters:
    - m = -(n * ln(p)) / (ln(2)^2)  (bit array size)
    - k = (m/n) * ln(2)  (number of hash functions)
    
    where:
    - n = expected number of elements
    - p = desired false positive rate
    """
    
    def __init__(self, expected_elements: int = 100000, false_positive_rate: float = 0.01):
        """
        Initialize Bloom filter.
        
        Args:
            expected_elements: Expected number of elements to store
            false_positive_rate: Desired false positive probability
        """
        import math
        
        # Calculate optimal bit array size
        self.size = int(- (expected_elements * math.log(false_positive_rate)) / (math.log(2) ** 2))
        self.size = (self.size + 7) & ~7  # Round up to nearest byte
        
        # Calculate optimal number of hash functions
        self.hash_count = int((self.size / expected_elements) * math.log(2))
        self.hash_count = max(1, self.hash_count)
        
        # Bit array
        self.bit_array = bytearray(self.size // 8)
        
        self.item_count = 0
    
    def _hash_functions(self, item: str) -> list[int]:
        """
        Generate k hash values using double hashing technique.
        
        Uses: h_i(x) = (h1(x) + i * h2(x)) mod m
        
        Args:
            item: String to hash
            
        Returns:
            List of k hash values
        """
        # Two base hash functions
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest()[:16], 16)
        
        hashes = []
        for i in range(self.hash_count):
            # Combine using double hashing
            h = (h1 + i * h2) % self.size
            hashes.append(h)
        
        return hashes
    
    def add(self, item: str) -> None:
        """
        Add item to Bloom filter.
        
        Args:
            item: String to add
        """
        for hash_value in self._hash_functions(item):
            index = hash_value // 8
            offset = hash_value % 8
            self.bit_array[index] |= (1 << offset)
        
        self.item_count += 1
    
    def contains(self, item: str) -> bool:
        """
        Test if item might be in the filter.
        
        Args:
            item: String to test
            
        Returns:
            True if item is probably in set, False if definitely not
        """
        for hash_value in self._hash_functions(item):
            index = hash_value // 8
            offset = hash_value % 8
            
            if not (self.bit_array[index] & (1 << offset)):
                return False
        
        return True


class CIDRMatcher:
    """
    Proper CIDR matching with bit-level operations.
    
    Implements correct IP address to integer conversion and
    subnet mask comparison.
    """
    
    @staticmethod
    def ip_to_int(ip: str) -> int:
        """
        Convert IP address string to integer.
        
        Args:
            ip: IP address (e.g., "192.168.1.1")
            
        Returns:
            Integer representation
        """
        parts = ip.strip().split(".")
        if len(parts) != 4:
            return 0
        
        try:
            return (int(parts[0]) << 24) | (int(parts[1]) << 16) | (int(parts[2]) << 8) | int(parts[3])
        except ValueError:
            return 0
    
    @staticmethod
    def parse_cidr(cidr: str) -> tuple[int, int]:
        """
        Parse CIDR notation to network address and mask.
        
        Args:
            cidr: CIDR notation (e.g., "192.168.1.0/24")
            
        Returns:
            Tuple of (network_address, subnet_mask)
        """
        if "/" not in cidr:
            return (CIDRMatcher.ip_to_int(cidr), 0xFFFFFFFF)
        
        ip_part, prefix_len = cidr.split("/")
        prefix_len = int(prefix_len)
        
        # Create subnet mask
        if prefix_len == 0:
            mask = 0
        else:
            mask = (0xFFFFFFFF << (32 - prefix_len)) & 0xFFFFFFFF
        
        network = CIDRMatcher.ip_to_int(ip_part) & mask
        
        return (network, mask)
    
    @staticmethod
    def ip_in_cidr(ip: str, cidr: str) -> bool:
        """
        Check if IP address is in CIDR range.
        
        Uses proper bit-level comparison.
        
        Args:
            ip: IP address to check
            cidr: CIDR range
            
        Returns:
            True if IP is in range
        """
        ip_int = CIDRMatcher.ip_to_int(ip)
        network, mask = CIDRMatcher.parse_cidr(cidr)
        
        return (ip_int & mask) == network


class LevenshteinDistance:
    """
    Levenshtein distance for typosquatting detection.
    
    Minimum number of single-character edits (insertions,
    deletions, or substitutions) required to change one word
    into another.
    
    Uses Wagner-Fischer algorithm with O(m*n) time complexity
    and O(min(m,n)) space optimization.
    """
    
    @staticmethod
    def distance(s1: str, s2: str) -> int:
        """
        Compute Levenshtein distance between two strings.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Edit distance
        """
        if len(s1) < len(s2):
            return LevenshteinDistance.distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        # Previous row of distances
        previous_row = list(range(len(s2) + 1))
        
        for i, c1 in enumerate(s1):
            # Current row
            current_row = [i + 1]
            
            for j, c2 in enumerate(s2):
                # Calculate costs
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                
                current_row.append(min(insertions, deletions, substitutions))
            
            previous_row = current_row
        
        return previous_row[-1]
    
    @staticmethod
    def similarity(s1: str, s2: str) -> float:
        """
        Compute normalized similarity score.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Similarity in [0, 1]
        """
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 1.0
        
        distance = LevenshteinDistance.distance(s1, s2)
        return 1.0 - (distance / max_len)


class DGADetector:
    """
    Domain Generation Algorithm detection using entropy and pattern analysis.
    
    DGA domains typically have:
    - High entropy (randomness)
    - Unusual length distributions
    - Mix of consonants and numbers
    - No dictionary words
    """
    
    # Common English words (for dictionary check)
    COMMON_WORDS = {
        "the", "and", "for", "are", "but", "not", "you", "all", "any", "can",
        "had", "her", "was", "one", "our", "out", "day", "get", "has", "him",
        "his", "how", "man", "new", "now", "old", "see", "two", "way", "who",
        "did", "its", "let", "put", "say", "she", "too", "use", "www", "com",
        "org", "net", "mail", "login", "secure", "account", "service", "bank"
    }
    
    @staticmethod
    def shannon_entropy(s: str) -> float:
        """
        Calculate Shannon entropy of a string.
        
        H(X) = -Σ P(x_i) * log2(P(x_i))
        
        Args:
            s: Input string
            
        Returns:
            Entropy in bits
        """
        if not s:
            return 0.0
        
        # Frequency count
        freq = {}
        for c in s:
            freq[c] = freq.get(c, 0) + 1
        
        length = len(s)
        entropy = 0.0
        
        for count in freq.values():
            probability = count / length
            if probability > 0:
                entropy -= probability * math.log2(probability)
        
        return entropy
    
    @staticmethod
    def is_dga_domain(domain: str) -> tuple[bool, dict[str, Any]]:
        """
        Detect if domain looks like it was generated by a DGA.
        
        Args:
            domain: Domain name to analyze
            
        Returns:
            Tuple of (is_dga, analysis_details)
        """
        # Extract domain name (remove TLD)
        parts = domain.lower().split(".")
        if len(parts) < 2:
            return (False, {"reason": "Invalid domain format"})
        
        # Use the main domain part (not TLD)
        name = parts[-2] if len(parts) >= 2 else parts[0]
        
        indicators = []
        dga_score = 0.0
        
        # 1. Shannon entropy (DGA domains have high entropy)
        entropy = DGADetector.shannon_entropy(name)
        if entropy > 3.5:
            dga_score += 0.3
            indicators.append(f"High entropy: {entropy:.2f}")
        
        # 2. Length check (DGA domains are often long)
        if len(name) > 12:
            dga_score += 0.2
            indicators.append(f"Long domain name: {len(name)} chars")
        
        # 3. Digit ratio (DGA mixes letters and numbers)
        digit_count = sum(1 for c in name if c.isdigit())
        digit_ratio = digit_count / len(name) if name else 0
        
        if 0.2 < digit_ratio < 0.6:
            dga_score += 0.2
            indicators.append(f"Digit ratio: {digit_ratio:.2f}")
        
        # 4. Consonant/vowel ratio (DGA has unusual ratios)
        vowels = set("aeiou")
        vowel_count = sum(1 for c in name if c in vowels)
        consonant_count = sum(1 for c in name if c.isalpha() and c not in vowels)
        
        if consonant_count > 0 and vowel_count > 0:
            cv_ratio = consonant_count / vowel_count
            if cv_ratio > 3.0 or cv_ratio < 0.5:
                dga_score += 0.15
                indicators.append(f"Unusual C/V ratio: {cv_ratio:.2f}")
        
        # 5. Dictionary word check
        if name not in DGADetector.COMMON_WORDS:
            # Check if any substring is a common word
            has_word = any(
                word in name for word in DGADetector.COMMON_WORDS
                if len(word) > 3
            )
            if not has_word:
                dga_score += 0.15
                indicators.append("No dictionary words found")
        
        is_dga = dga_score > 0.5
        
        return (is_dga, {
            "dga_score": dga_score,
            "entropy": entropy,
            "indicators": indicators,
            "length": len(name),
            "digit_ratio": digit_ratio
        })


class ThreatIntelMatcher:
    """
    Threat intelligence matcher backed by real open threat feeds.

    Matching sources are ordered as:
    1. Local cached feed files under `~/.network-traffic-analysis/feeds` or
       `NETWORK_TRAFFIC_THREAT_FEEDS_DIR`
    2. Refresh from public open-source threat feeds

    If both are unavailable, the matcher returns `no_feed` coverage status and
    does not fabricate IOC coverage. Heuristic detections such as DGA and
    typosquatting remain available, but they are labeled as heuristic output.
    """

    DEFAULT_FEED_DIR = Path.home() / ".network-traffic-analysis" / "feeds"
    NO_FEED_WARNING = (
        "No real threat feed is loaded. IOC matching coverage is unavailable. "
        "Populate the local cache or allow refresh from public open-source feeds."
    )
    OPEN_SOURCE_FEEDS = [
        {
            "name": "abuse_ch_feodo",
            "url": "https://feodotracker.abuse.ch/downloads/ipblocklist.json",
            "filename": "abuse_ch_feodo.json",
        },
        {
            "name": "abuse_ch_threatfox",
            "url": "https://threatfox.abuse.ch/export/json/recent/",
            "filename": "abuse_ch_threatfox.json",
        },
        {
            "name": "abuse_ch_urlhaus",
            "url": "https://urlhaus.abuse.ch/downloads/json/",
            "filename": "abuse_ch_urlhaus.json",
        },
        {
            "name": "tor_exit_nodes",
            "url": "https://check.torproject.org/torbulkexitlist",
            "filename": "tor_exit_nodes.txt",
        },
    ]
    SEVERITY_TO_SCORE = {
        "critical": 95.0,
        "high": 85.0,
        "medium": 70.0,
        "low": 55.0,
        "info": 35.0,
    }
    INDICATOR_VALUE_KEYS = {
        "entity",
        "indicator",
        "ioc",
        "ioc_value",
        "value",
        "ip",
        "ip_address",
        "dst_ip",
        "src_ip",
        "domain",
        "hostname",
        "fqdn",
        "host",
        "domain_name",
        "url",
        "cidr",
        "network",
        "subnet",
    }

    def __init__(self):
        self.malicious_ip_bloom = BloomFilter(expected_elements=100000, false_positive_rate=0.001)
        self.malicious_domain_bloom = BloomFilter(expected_elements=100000, false_positive_rate=0.001)

        self.malicious_cidrs: list[dict[str, Any]] = []
        self.malicious_domains: dict[str, dict[str, Any]] = {}
        self.malicious_ips: dict[str, dict[str, Any]] = {}

        # Load externalized data files
        self.data_directory = Path(__file__).resolve().parents[2] / "data"
        self.known_brands = self._load_external_brands()
        self.mitre_database = self._load_external_mitre_database()
        self.feed_directory = Path(
            os.environ.get("NETWORK_TRAFFIC_THREAT_FEEDS_DIR", str(self.DEFAULT_FEED_DIR))
        ).expanduser()
        self.loaded_feeds: list[dict[str, Any]] = []
        self.refresh_errors: list[str] = []
        self.total_loaded_indicators = 0
        self.data_mode = "no_feed"
        self.coverage_warning = self.NO_FEED_WARNING

        loaded_any = self._load_local_feeds()
        auto_refresh = os.environ.get("NETWORK_TRAFFIC_THREAT_AUTO_REFRESH", "").lower() in {"1", "true", "yes"}
        if auto_refresh:
            self._refresh_and_reload_open_source_feeds()

    def _load_external_brands(self) -> list[str]:
        """Load known brand names for typosquatting detection.

        Brand names are configuration rather than downloadable data.
        Uses a curated list of commonly targeted brands in phishing and
        typosquatting campaigns.
        """
        return [
            "google", "microsoft", "apple", "amazon", "facebook",
            "netflix", "paypal", "twitter", "linkedin", "github",
            "dropbox", "adobe", "oracle", "ibm", "cisco",
            "zoom", "slack", "whatsapp", "telegram", "signal",
            "tiktok", "instagram", "reddit", "youtube", "spotify",
            "chase", "wellsfargo", "citibank", "hsbc",
            "fedex", "ups", "dhl", "usps", "amazonaws",
        ]

    def _load_external_mitre_database(self) -> dict[str, dict[str, Any]]:
        """Load MITRE ATT&CK techniques from real external data file.

        Source: extracted from MITRE CTI STIX repository (enterprise-attack v15.1).
        URL: https://github.com/mitre/cti
        Extraction script: downloads the STIX bundle and flattens attack-pattern objects
        to a lightweight JSON keyed by technique ID (e.g., T1071, T1568.002).

        Raises:
            FileNotFoundError: If the external MITRE data file is missing.
        """
        mitre_path = self.data_directory / "external" / "mitre_attack_techniques.json"
        if not mitre_path.exists():
            raise FileNotFoundError(
                f"MITRE ATT&CK data file not found at {mitre_path}. "
                "Download it from https://github.com/mitre/cti and extract techniques "
                "to data/external/mitre_attack_techniques.json."
            )
        try:
            payload = json.loads(mitre_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Failed to parse MITRE ATT&CK data from {mitre_path}: {exc}"
            ) from exc
        return payload

    def _load_local_feeds(self) -> bool:
        if not self.feed_directory.exists() or not self.feed_directory.is_dir():
            return False

        loaded_any = False
        for path in sorted(self.feed_directory.iterdir()):
            if not path.is_file() or path.suffix.lower() not in {".json", ".jsonl", ".txt"}:
                continue
            indicators = self._load_feed_file(path)
            if not indicators:
                continue

            for indicator in indicators:
                self._register_indicator(indicator)

            self.loaded_feeds.append(
                {
                    "name": path.stem,
                    "path": str(path),
                    "indicator_count": len(indicators),
                    "cache_age_hours": self._cache_age_hours(path),
                }
            )
            loaded_any = True

        if loaded_any:
            self.data_mode = "local_cache"
            self.coverage_warning = ""
        return loaded_any

    def _refresh_and_reload_open_source_feeds(self) -> bool:
        self.feed_directory.mkdir(parents=True, exist_ok=True)
        refreshed = False
        for feed in self.OPEN_SOURCE_FEEDS:
            try:
                self._download_feed(feed)
                refreshed = True
            except Exception as exc:
                self.refresh_errors.append(f"{feed['name']}: {exc}")

        if not refreshed:
            if self.refresh_errors:
                self.coverage_warning = (
                    "No real threat feed is loaded. Refresh from public open-source feeds failed: "
                    + "; ".join(self.refresh_errors)
                )
            return False

        self._reset_loaded_state()
        loaded_any = self._load_local_feeds()
        if loaded_any:
            self.data_mode = "refreshed_cache"
            self.coverage_warning = ""
        return loaded_any

    def _download_feed(self, feed: dict[str, str]) -> None:
        request = Request(feed["url"], headers={"User-Agent": "network-traffic-analysis/1.0"})
        try:
            with urlopen(request, timeout=20) as response:
                payload = response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"download failed for {feed['url']}: {exc}") from exc

        target = self.feed_directory / feed["filename"]
        target.write_bytes(payload)

    def _reset_loaded_state(self) -> None:
        self.malicious_ip_bloom = BloomFilter(expected_elements=100000, false_positive_rate=0.001)
        self.malicious_domain_bloom = BloomFilter(expected_elements=100000, false_positive_rate=0.001)
        self.malicious_cidrs = []
        self.malicious_domains = {}
        self.malicious_ips = {}
        self.loaded_feeds = []
        self.total_loaded_indicators = 0

    def _load_feed_file(self, path: Path) -> list[IndicatorRecord]:
        if path.suffix.lower() == ".txt":
            return self._load_text_feed(path)
        if path.suffix.lower() == ".jsonl":
            return self._load_jsonl_feed(path)
        return self._load_json_feed(path)

    def _load_text_feed(self, path: Path) -> list[IndicatorRecord]:
        indicators: list[IndicatorRecord] = []
        source_url = self._default_source_url(path.stem)
        threat_type = "tor_exit_node" if "tor" in path.stem.lower() else "feed_indicator"
        severity = "medium" if "tor" in path.stem.lower() else "low"
        cache_age_hours = self._cache_age_hours(path)

        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            indicators.append(
                IndicatorRecord(
                    entity=line,
                    entity_type=self._infer_entity_type(line),
                    threat_type=threat_type,
                    severity=severity,
                    source=path.stem,
                    source_url=source_url,
                    description=f"Loaded from local text feed {path.name}",
                    confidence=0.75,
                    cache_age_hours=cache_age_hours,
                    reputation_score=self._severity_score(severity),
                )
            )
        return indicators

    def _load_json_feed(self, path: Path) -> list[IndicatorRecord]:
        raw_bytes = path.read_bytes()
        if raw_bytes.startswith(b"PK\x03\x04"):
            return self._load_zip_feed(path)

        text = raw_bytes.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return []
        return self._indicators_from_json_payload(
            payload,
            source_name=path.stem,
            source_url=self._default_source_url(path.stem),
            cache_age_hours=self._cache_age_hours(path),
        )

    def _load_jsonl_feed(self, path: Path) -> list[IndicatorRecord]:
        cache_age_hours = self._cache_age_hours(path)
        source_url = self._default_source_url(path.stem)
        indicators: list[IndicatorRecord] = []
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            indicator = self._indicator_from_record(record, path.stem, source_url, cache_age_hours)
            if indicator is not None:
                indicators.append(indicator)
        return indicators

    def _load_zip_feed(self, path: Path) -> list[IndicatorRecord]:
        cache_age_hours = self._cache_age_hours(path)
        source_url = self._default_source_url(path.stem)
        indicators: list[IndicatorRecord] = []
        try:
            with zipfile.ZipFile(path) as archive:
                for member_name in sorted(archive.namelist()):
                    member = member_name.lower()
                    if member.endswith("/"):
                        continue
                    with archive.open(member_name) as handle:
                        payload_bytes = handle.read()
                    if member.endswith(".json"):
                        try:
                            payload = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
                        except json.JSONDecodeError:
                            continue
                        indicators.extend(
                            self._indicators_from_json_payload(
                                payload,
                                source_name=path.stem,
                                source_url=source_url,
                                cache_age_hours=cache_age_hours,
                            )
                        )
                    elif member.endswith(".jsonl"):
                        for raw_line in payload_bytes.decode("utf-8", errors="ignore").splitlines():
                            line = raw_line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            indicator = self._indicator_from_record(
                                record,
                                path.stem,
                                source_url,
                                cache_age_hours,
                            )
                            if indicator is not None:
                                indicators.append(indicator)
                    elif member.endswith(".txt"):
                        for raw_line in payload_bytes.decode("utf-8", errors="ignore").splitlines():
                            line = raw_line.strip()
                            if not line or line.startswith("#"):
                                continue
                            indicators.append(
                                IndicatorRecord(
                                    entity=line,
                                    entity_type=self._infer_entity_type(line),
                                    threat_type="feed_indicator",
                                    severity="low",
                                    source=path.stem,
                                    source_url=source_url,
                                    description=f"Loaded from archive member {member_name}",
                                    confidence=0.75,
                                    cache_age_hours=cache_age_hours,
                                    reputation_score=self._severity_score("low"),
                                )
                            )
        except zipfile.BadZipFile:
            return []
        return indicators

    def _indicators_from_json_payload(
        self,
        payload: Any,
        *,
        source_name: str,
        source_url: str,
        cache_age_hours: float | None,
    ) -> list[IndicatorRecord]:
        indicators: list[IndicatorRecord] = []
        for record in self._iter_json_records(payload):
            indicator = self._indicator_from_record(record, source_name, source_url, cache_age_hours)
            if indicator is not None:
                indicators.append(indicator)
        return indicators

    def _iter_json_records(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            records: list[dict[str, Any]] = []
            for item in payload:
                records.extend(self._iter_json_records(item))
            return records

        if not isinstance(payload, dict):
            return []

        if any(key in payload for key in self.INDICATOR_VALUE_KEYS):
            return [payload]

        records: list[dict[str, Any]] = []
        for value in payload.values():
            if isinstance(value, (list, dict)):
                records.extend(self._iter_json_records(value))
        return records

    def _indicator_from_record(
        self,
        record: dict[str, Any],
        source_name: str,
        source_url: str,
        cache_age_hours: float | None,
    ) -> IndicatorRecord | None:
        entity = self._normalize_indicator_value(
            self._first_nonempty(
                record,
                (
                    "entity",
                    "indicator",
                    "ioc",
                    "ioc_value",
                    "value",
                    "ip",
                    "ip_address",
                    "dst_ip",
                    "src_ip",
                    "domain",
                    "hostname",
                    "fqdn",
                    "host",
                    "domain_name",
                    "url",
                    "cidr",
                    "network",
                    "subnet",
                ),
            )
        )
        if not entity:
            return None

        explicit_type = self._first_nonempty(record, ("type", "ioc_type", "entity_type", "indicator_type"))
        entity_type = self._infer_entity_type(entity, explicit_type)
        threat_type = self._normalize_threat_type(
            record,
            source_name,
        )
        severity = (self._first_nonempty(record, ("severity", "priority", "risk", "confidence_level")) or "").lower()
        if severity not in {"critical", "high", "medium", "low", "info"}:
            severity = self._severity_from_threat_type(threat_type)

        return IndicatorRecord(
            entity=entity,
            entity_type=entity_type,
            threat_type=threat_type,
            severity=severity,
            source=self._first_nonempty(record, ("source", "feed", "provider")) or source_name,
            source_url=self._first_nonempty(record, ("source_url", "reference", "reference_url")) or source_url,
            description=self._first_nonempty(record, ("description", "comment", "details")) or f"Loaded from threat feed {source_name}",
            mitre_techniques=self._normalize_list(record.get("mitre_techniques") or record.get("mitre")),
            campaigns=self._normalize_list(record.get("campaigns") or record.get("campaign")),
            confidence=self._normalize_confidence(record.get("confidence") or record.get("confidence_level"), default=0.75),
            first_seen=self._first_nonempty(record, ("first_seen", "first_seen_utc", "date_added", "dateadded", "seen_first")),
            last_seen=self._first_nonempty(record, ("last_seen", "last_seen_utc", "last_online", "date_updated", "seen_last")),
            tags=self._normalize_list(record.get("tags")),
            ttl_hours=self._safe_int(record.get("ttl_hours")),
            cache_age_hours=cache_age_hours,
            reputation_score=self._safe_float(record.get("reputation_score"), default=self._severity_score(severity)),
        )

    def _register_indicator(self, indicator: IndicatorRecord) -> None:
        record = asdict(indicator)
        entity_key = indicator.entity.lower() if indicator.entity_type in {"domain", "url"} else indicator.entity

        if indicator.entity_type == "ip":
            self.malicious_ips[entity_key] = record
            self.malicious_ip_bloom.add(entity_key)
        elif indicator.entity_type in {"domain", "url"}:
            self.malicious_domains[entity_key] = record
            self.malicious_domain_bloom.add(entity_key)
        elif indicator.entity_type == "cidr":
            self.malicious_cidrs.append(record)

        self.total_loaded_indicators += 1

    def _match_single_ioc(self, entity: str, entity_type: str) -> ThreatIntelResult:
        if entity_type == "ip":
            return self._analyze_ip(entity)
        if entity_type == "domain":
            return self._analyze_domain(entity)
        if entity_type == "url":
            domain = entity.split("/")[2] if "/" in entity else entity
            return self._analyze_domain(domain)

        return ThreatIntelResult(
            entity_id=entity,
            entity_type=entity_type,
            matched=False,
            threat_type="unknown",
            severity="info",
            reputation_score=50.0,
            mitre_techniques=[],
            campaigns=[],
            enrichment={},
            recommended_actions=[],
            coverage_mode=self.data_mode,
        )

    def match_iocs(self, indicators: list[dict[str, Any]]) -> list[ThreatIntelResult]:
        results = []
        for indicator in indicators:
            entity = str(indicator.get("entity", "")).strip()
            entity_type = indicator.get("type", "unknown")
            if not entity:
                continue
            results.append(self._match_single_ioc(entity, entity_type))
        return results

    def _analyze_ip(self, ip: str) -> ThreatIntelResult:
        if self.malicious_ip_bloom.contains(ip) and ip in self.malicious_ips:
            return self._result_from_indicator(ip, "ip", self.malicious_ips[ip])

        for cidr_info in self.malicious_cidrs:
            cidr = str(cidr_info.get("entity", ""))
            if cidr and CIDRMatcher.ip_in_cidr(ip, cidr):
                enrichment = {"cidr_match": cidr, "description": cidr_info.get("description", "")}
                return self._result_from_indicator(
                    ip,
                    "ip",
                    cidr_info,
                    enrichment=enrichment,
                    recommended_actions=[
                        f"Investigate communication with IPs in malicious range {cidr}",
                        "Review historical flows for repeated contact",
                    ],
                )

        return self._create_clean_ip_result(ip)

    def _analyze_domain(self, domain: str) -> ThreatIntelResult:
        domain_lower = domain.lower()

        if self.malicious_domain_bloom.contains(domain_lower) and domain_lower in self.malicious_domains:
            return self._result_from_indicator(domain, "domain", self.malicious_domains[domain_lower])

        for brand in self.known_brands:
            similarity = LevenshteinDistance.similarity(domain_lower, f"{brand}.com")
            if 0.8 < similarity < 1.0:
                distance = LevenshteinDistance.distance(domain_lower, f"{brand}.com")
                if distance <= 2:
                    return ThreatIntelResult(
                        entity_id=domain,
                        entity_type="domain",
                        matched=True,
                        threat_type="typosquatting",
                        severity="high",
                        reputation_score=85.0,
                        mitre_techniques=["T1598.003"],
                        campaigns=[],
                        enrichment={
                            "similar_brand": brand,
                            "similarity_score": round(similarity, 4),
                            "edit_distance": distance,
                        },
                        recommended_actions=[
                            f"Investigate possible typosquatting of {brand}",
                            "Check certificate issuer and domain registration context",
                        ],
                        source="heuristic_typosquat",
                        source_url="local://heuristic/levenshtein",
                        confidence=0.7,
                        coverage_mode="heuristic",
                    )

        is_dga, dga_details = DGADetector.is_dga_domain(domain_lower)
        if is_dga:
            return ThreatIntelResult(
                entity_id=domain,
                entity_type="domain",
                matched=True,
                threat_type="dga_domain",
                severity="high",
                reputation_score=80.0,
                mitre_techniques=["T1568.002"],
                campaigns=[],
                enrichment={"dga_analysis": dga_details},
                recommended_actions=[
                    "Investigate querying hosts for malware or beaconing activity",
                    "Review DNS query timing and NXDOMAIN behavior",
                ],
                source="heuristic_dga",
                source_url="local://heuristic/dga",
                confidence=round(min(max(dga_details.get("dga_score", 0.0), 0.55), 0.95), 4),
                coverage_mode="heuristic",
            )

        return self._create_clean_domain_result(domain)

    def _result_from_indicator(
        self,
        entity: str,
        entity_type: str,
        indicator: dict[str, Any],
        *,
        enrichment: dict[str, Any] | None = None,
        recommended_actions: list[str] | None = None,
    ) -> ThreatIntelResult:
        severity = str(indicator.get("severity", "medium")).lower()
        actions = recommended_actions or self._default_actions(indicator)
        return ThreatIntelResult(
            entity_id=entity,
            entity_type=entity_type,
            matched=True,
            threat_type=str(indicator.get("threat_type", "feed_match")),
            severity=severity,
            reputation_score=self._safe_float(indicator.get("reputation_score"), default=self._severity_score(severity)),
            mitre_techniques=self._normalize_list(indicator.get("mitre_techniques")),
            campaigns=self._normalize_list(indicator.get("campaigns")),
            enrichment=enrichment or {
                key: value
                for key, value in indicator.items()
                if key not in {"entity", "entity_type", "source", "source_url", "cache_age_hours", "reputation_score"}
            },
            recommended_actions=actions,
            source=str(indicator.get("source", "")),
            source_url=str(indicator.get("source_url", "")),
            first_seen=indicator.get("first_seen"),
            last_seen=indicator.get("last_seen"),
            confidence=self._normalize_confidence(indicator.get("confidence"), default=0.75),
            cache_age_hours=self._safe_float(indicator.get("cache_age_hours"), default=None),
            coverage_mode=self.data_mode,
        )

    def _default_actions(self, indicator: dict[str, Any]) -> list[str]:
        actions: list[str] = []
        severity = str(indicator.get("severity", "medium")).lower()
        if severity in {"critical", "high"}:
            actions.append("Investigate all hosts that communicated with this entity")
            actions.append("Review blocking and containment options if not business-critical")
        else:
            actions.append("Review traffic context and confirm whether communication is expected")
        return actions

    def _create_clean_ip_result(self, ip: str) -> ThreatIntelResult:
        is_private = (
            ip.startswith("10.")
            or ip.startswith("172.16.") or ip.startswith("172.17.")
            or ip.startswith("172.18.") or ip.startswith("172.19.")
            or ip.startswith("172.20.") or ip.startswith("172.21.")
            or ip.startswith("172.22.") or ip.startswith("172.23.")
            or ip.startswith("172.24.") or ip.startswith("172.25.")
            or ip.startswith("172.26.") or ip.startswith("172.27.")
            or ip.startswith("172.28.") or ip.startswith("172.29.")
            or ip.startswith("172.30.") or ip.startswith("172.31.")
            or ip.startswith("192.168.")
        )
        return ThreatIntelResult(
            entity_id=ip,
            entity_type="ip",
            matched=False,
            threat_type="unknown",
            severity="info",
            reputation_score=20.0 if is_private else 40.0,
            mitre_techniques=[],
            campaigns=[],
            enrichment={"is_private": is_private},
            recommended_actions=[],
            coverage_mode=self.data_mode,
        )

    def _create_clean_domain_result(self, domain: str) -> ThreatIntelResult:
        return ThreatIntelResult(
            entity_id=domain,
            entity_type="domain",
            matched=False,
            threat_type="unknown",
            severity="info",
            reputation_score=30.0,
            mitre_techniques=[],
            campaigns=[],
            enrichment={},
            recommended_actions=[],
            coverage_mode=self.data_mode,
        )

    def score_reputation(self, entity: str, entity_type: str) -> dict[str, Any]:
        result = self._match_single_ioc(entity, entity_type)
        return {
            "reputation_score": result.reputation_score,
            "threat_type": result.threat_type,
            "severity": result.severity,
            "matched": result.matched,
            "source": result.source,
            "coverage_mode": result.coverage_mode,
        }

    def get_status(self) -> dict[str, Any]:
        return {
            "coverage_mode": self.data_mode,
            "coverage_warning": self.coverage_warning,
            "loaded_feed_count": len(self.loaded_feeds),
            "total_loaded_indicators": self.total_loaded_indicators,
            "feed_directory": str(self.feed_directory),
            "refresh_errors": self.refresh_errors,
        }

    def map_to_mitre(self, techniques: list[str]) -> list[dict[str, Any]]:
        mapped = []
        for tech_id in techniques:
            if tech_id in self.mitre_database:
                tech_info = self.mitre_database[tech_id]
                mapped.append(
                    {
                        "technique_id": tech_id,
                        "name": tech_info["name"],
                        "tactic": tech_info["tactic"],
                        "description": tech_info["description"],
                    }
                )
        return mapped

    def _first_nonempty(self, record: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = record.get(key)
            if value in (None, ""):
                continue
            return str(value).strip()
        return ""

    def _normalize_indicator_value(self, value: str) -> str:
        text = value.strip()
        if not text:
            return ""
        if text.startswith(("http://", "https://")):
            host = text.split("://", 1)[1].split("/", 1)[0]
            return host.strip()
        if ":" in text and "/" not in text:
            host, port = text.rsplit(":", 1)
            if port.isdigit() and CIDRMatcher.ip_to_int(host) > 0:
                return host
        return text

    def _normalize_threat_type(self, record: dict[str, Any], source_name: str) -> str:
        threat_type = self._first_nonempty(
            record,
            ("threat_type", "threat", "malware", "malware_family", "category", "ioc_type", "classification", "tag"),
        )
        if not threat_type:
            return source_name.replace("-", "_")
        normalized = threat_type.lower().replace(" ", "_")
        if normalized in {"ip", "ip:port", "domain", "url"}:
            return source_name.replace("-", "_")
        return normalized

    def _normalize_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return [str(value).strip()]

    def _safe_float(self, value: Any, *, default: float | None) -> float | None:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _normalize_confidence(self, value: Any, *, default: float) -> float:
        parsed = self._safe_float(value, default=default)
        if parsed is None:
            return default
        if 1.0 < parsed <= 100.0:
            return round(parsed / 100.0, 4)
        return round(max(0.0, min(parsed, 1.0)), 4)

    def _safe_int(self, value: Any) -> int | None:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _cache_age_hours(self, path: Path) -> float | None:
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return None
        return round((datetime.now(timezone.utc) - modified).total_seconds() / 3600.0, 2)

    def _infer_entity_type(self, entity: str, explicit_type: str = "") -> str:
        normalized = explicit_type.strip().lower()
        if normalized in {"ip:port", "ipv4"}:
            return "ip"
        if normalized in {"domain_name", "hostname"}:
            return "domain"
        if normalized in {"ip", "domain", "url", "hash", "cidr"}:
            return normalized

        stripped = entity.strip().lower()
        if stripped.startswith(("http://", "https://")):
            return "url"
        if "/" in stripped:
            ip_part = stripped.split("/", 1)[0]
            if CIDRMatcher.ip_to_int(ip_part) > 0:
                return "cidr"
        if CIDRMatcher.ip_to_int(stripped) > 0:
            return "ip"
        return "domain"

    def _severity_from_threat_type(self, threat_type: str) -> str:
        value = threat_type.lower()
        if any(token in value for token in ("c2", "phish", "malware", "botnet")):
            return "high"
        if any(token in value for token in ("tor", "crypto", "mine", "scanner")):
            return "medium"
        return "low"

    def _severity_score(self, severity: str) -> float:
        return self.SEVERITY_TO_SCORE.get(severity.lower(), 50.0)

    def _default_source_url(self, source_name: str) -> str:
        lowered = source_name.lower()
        if "tor" in lowered:
            return "https://check.torproject.org/torbulkexitlist"
        if any(token in lowered for token in ("feodo", "threatfox", "urlhaus", "abuse")):
            return "https://abuse.ch/"
        return ""
