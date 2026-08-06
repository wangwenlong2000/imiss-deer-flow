"""Deterministic, auditable holdout splitting for compliance samples."""
from __future__ import annotations

import hashlib
import json
import math
import random
from collections import Counter, defaultdict
from typing import Iterable


LABEL_FIELDS = ("target_violation_type", "final_violation_type", "is_positive", "sample_kind")
STRATUM_FIELDS = ("target_violation_type", "is_positive", "sample_kind", "data_type")
COVERAGE_FIELDS = ("sample_kind", "data_type", "is_positive")


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_sha256(row: dict) -> str:
    if "raw_content" not in row:
        raise ValueError("row is missing raw_content")
    return hashlib.sha256(_canonical_json(row["raw_content"]).encode("utf-8")).hexdigest()


def deduplicate_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    unique: list[dict] = []
    removed: list[dict] = []
    by_hash: dict[str, dict] = {}
    labels_by_hash: dict[str, tuple[object, ...]] = {}
    for row in rows:
        digest = content_sha256(row)
        labels = tuple(row.get(field) for field in LABEL_FIELDS)
        if digest not in by_hash:
            by_hash[digest] = row
            labels_by_hash[digest] = labels
            unique.append(row)
            continue
        if labels != labels_by_hash[digest]:
            raise ValueError(
                f"label conflict for raw_content SHA-256 {digest}: "
                f"{labels_by_hash[digest]!r} != {labels!r}"
            )
        removed.append(row)
    return unique, removed


def _stratum(row: dict) -> tuple[object, ...]:
    return tuple(row.get(field) for field in STRATUM_FIELDS)


def _stable_shuffle(rows: Iterable[dict], seed: int, salt: object) -> list[dict]:
    material = f"{seed}:{_canonical_json(salt)}".encode("utf-8")
    derived_seed = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    output = list(rows)
    random.Random(derived_seed).shuffle(output)
    return output


def _allocate_counts(groups: dict[tuple[object, ...], list[dict]], quota: int, ratio: float) -> dict:
    allocations = {key: math.floor(len(items) * ratio) for key, items in groups.items()}
    remaining = quota - sum(allocations.values())
    ranked = sorted(
        groups,
        key=lambda key: (-(len(groups[key]) * ratio - allocations[key]), _canonical_json(key)),
    )
    for key in ranked[:remaining]:
        allocations[key] += 1
    return allocations


def _coverage_counts(groups: dict, allocations: dict) -> dict[str, Counter]:
    counts = {field: Counter() for field in COVERAGE_FIELDS}
    field_indexes = {field: STRATUM_FIELDS.index(field) for field in COVERAGE_FIELDS}
    for key, count in allocations.items():
        if count <= 0:
            continue
        for field, index in field_indexes.items():
            counts[field][key[index]] += count
    return counts


def _ensure_marginal_coverage(groups: dict, allocations: dict) -> None:
    field_indexes = {field: STRATUM_FIELDS.index(field) for field in COVERAGE_FIELDS}
    required = {
        field: {key[index] for key in groups}
        for field, index in field_indexes.items()
    }
    for field in COVERAGE_FIELDS:
        for value in sorted(required[field], key=str):
            coverage = _coverage_counts(groups, allocations)
            if coverage[field][value] > 0:
                continue
            candidate_keys = sorted(
                (
                    key
                    for key in groups
                    if key[field_indexes[field]] == value and allocations[key] < len(groups[key])
                ),
                key=_canonical_json,
            )
            if not candidate_keys:
                raise ValueError(f"cannot cover {field}={value!r}")
            candidate = candidate_keys[0]

            donors: list[tuple[object, ...]] = []
            for donor in sorted(groups, key=_canonical_json):
                if allocations[donor] <= 0 or donor == candidate:
                    continue
                safe = True
                for protected_field, index in field_indexes.items():
                    protected_value = donor[index]
                    if coverage[protected_field][protected_value] <= 1:
                        safe = False
                        break
                if safe:
                    donors.append(donor)
            if not donors:
                raise ValueError(f"cannot preserve quota while covering {field}={value!r}")
            allocations[donors[0]] -= 1
            allocations[candidate] += 1


def _count_map(rows: list[dict], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(field)) for row in rows).items()))


def summarize_rows(rows: list[dict]) -> dict:
    return {
        "rows": len(rows),
        "by_target_violation_type": _count_map(rows, "target_violation_type"),
        "by_sample_kind": _count_map(rows, "sample_kind"),
        "by_is_positive": _count_map(rows, "is_positive"),
        "by_data_type": _count_map(rows, "data_type"),
        "by_gate": _count_map(rows, "gate"),
    }


def split_holdout(
    rows: list[dict], test_ratio: float = 0.2, seed: int = 20260701
) -> tuple[list[dict], list[dict], dict]:
    if not 0 < test_ratio < 1:
        raise ValueError("test_ratio must be between 0 and 1")
    by_target: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_target[str(row.get("target_violation_type"))].append(row)

    train: list[dict] = []
    test: list[dict] = []
    quotas: dict[str, int] = {}
    for target in sorted(by_target):
        target_rows = by_target[target]
        quota = math.floor(len(target_rows) * test_ratio + 0.5)
        quotas[target] = quota
        groups: dict[tuple[object, ...], list[dict]] = defaultdict(list)
        for row in target_rows:
            groups[_stratum(row)].append(row)
        allocations = _allocate_counts(groups, quota, test_ratio)
        _ensure_marginal_coverage(groups, allocations)
        for key in sorted(groups, key=_canonical_json):
            shuffled = _stable_shuffle(groups[key], seed, key)
            cut = allocations[key]
            test.extend(shuffled[:cut])
            train.extend(shuffled[cut:])

    train = _stable_shuffle(train, seed, "train")
    test = _stable_shuffle(test, seed, "test")
    summary = {
        "seed": seed,
        "test_ratio": test_ratio,
        "test_quotas": quotas,
        "train": summarize_rows(train),
        "test": summarize_rows(test),
        "overlap_validation": validate_no_overlap(train, test),
    }
    return train, test, summary


def validate_no_overlap(train: list[dict], test: list[dict]) -> dict[str, int]:
    result: dict[str, int] = {}
    for field in ("data_id", "original_data_id", "sample_id"):
        train_values = {row.get(field) for row in train if row.get(field) not in (None, "")}
        test_values = {row.get(field) for row in test if row.get(field) not in (None, "")}
        result[field] = len(train_values & test_values)
    result["exact_raw_content_sha256"] = len(
        {content_sha256(row) for row in train} & {content_sha256(row) for row in test}
    )
    return result
