#!/usr/bin/env python3
"""平台层 wrapper：把（时空轨迹）skill 的原生输出目录转成统一 SkillResult。

task.md §4 + goal「中间件/过滤器在平台层一次实现，避免各 RAG 各做一遍」：
各 skill 无需改造，跑完 skill 后对其 --output-dir 调用本脚本，即在该目录写出
``skill_result.json``（标准 SkillResult，经 validate_skill_result 校验）。

用法::

    python3 emit_skill_result.py --output-dir <skill输出目录> --skill-name <name> \
        [--scenario S] [--capability C] [--data-type spatiotemporal_trajectory] \
        [--evidence-file main.jsonl] [--max-evidence 200]

退出码：0 校验通过；2 校验失败（仍会写出文件，便于排查）。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 本脚本在 skills/_shared/retrieval_protocol/ 内；把 skills/_shared 加进 sys.path。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from retrieval_protocol import (  # noqa: E402
    build_evidence_unit,
    build_geo_scope,
    build_key_metric,
    build_retrieval_evidence,
    build_skill_result,
    build_summary,
    classify_sensitivity,
    validate_skill_result,
)

SKIP_JSONL = {"skill_result.json"}
_GEOHASH_KEYS = ("geohash", "geohash6", "grid", "grid_id", "cell", "cell_id")
_LAT_KEYS = ("lat", "latitude", "center_lat")
_LON_KEYS = ("lon", "lng", "longitude", "center_lon")
_SCORE_KEYS = ("score", "similarity", "heat", "heat_score", "visit_count", "count",
               "anomaly_score", "weight", "flow", "flow_count", "activity_index",
               "entropy", "probability", "prob", "stay_minutes", "duration_minutes",
               "total_distance_km", "support", "frequency")
_ID_KEYS = ("evidence_id", "id", "geohash", "region", "user_id", "track_id",
            "od_id", "corridor_id", "pair", "name")
_TEXT_KEYS = ("landmark", "region", "geohash", "user_id", "venue_category",
              "visit_count", "unique_users", "stay_minutes", "score", "similarity",
              "entropy", "flow", "flow_count", "origin", "dest", "state",
              "probability", "anomaly_type", "reason")


def _num(v):
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _pick_evidence_file(output_dir: Path, explicit: str | None) -> Path | None:
    if explicit:
        p = output_dir / explicit
        return p if p.exists() else None
    cands = [
        p for p in sorted(output_dir.glob("*.jsonl"))
        if p.name not in SKIP_JSONL and "viz" not in p.name.lower()
    ]
    if not cands:
        return None
    # 选记录数（行数）最多的那个 jsonl 作为主证据。
    return max(cands, key=lambda p: len(_read_jsonl(p)) or p.stat().st_size)


def _score_of(rec: dict):
    for k in _SCORE_KEYS:
        if k in rec and _num(rec[k]) is not None:
            return _num(rec[k])
    return None


def _geo_of(rec: dict) -> dict:
    kw: dict = {}
    for k in _GEOHASH_KEYS:
        if rec.get(k) not in (None, ""):
            kw["geohash"] = str(rec[k])
            break
    if rec.get("landmark") not in (None, ""):
        kw["landmark"] = str(rec["landmark"])
    if rec.get("region") not in (None, ""):
        kw["district"] = str(rec["region"])
    if rec.get("city") not in (None, ""):
        kw["city"] = str(rec["city"])
    for la in _LAT_KEYS:
        if _num(rec.get(la)) is not None:
            kw["lat"] = _num(rec[la])
            break
    for lo in _LON_KEYS:
        if _num(rec.get(lo)) is not None:
            kw["lon"] = _num(rec[lo])
            break
    return build_geo_scope(**kw) if kw else {}


def _id_of(rec: dict, idx: int) -> str:
    for k in _ID_KEYS:
        if rec.get(k) not in (None, ""):
            return str(rec[k])
    return f"ev{idx}"


def _text_of(rec: dict, skill: str, idx: int) -> str:
    parts = [f"{k}={rec[k]}" for k in _TEXT_KEYS if rec.get(k) not in (None, "")]
    body = ", ".join(parts[:8]) if parts else f"evidence {_id_of(rec, idx)}"
    return f"[{skill}] {body}"


def _to_evidence(rec: dict, skill: str, data_type: str, idx: int) -> dict:
    geo = _geo_of(rec)
    ev_id = _id_of(rec, idx)
    text = _text_of(rec, skill, idx)
    # classify_sensitivity 读 **top-level** text/features（原始记录形态，与 adapters 一致），
    # 不是 build_evidence_unit 的 meta.features 形态。故喂原始记录而非已建 unit。
    try:
        level, policy = classify_sensitivity(
            data_type=data_type, evidence_unit={"text": text, "features": rec}
        )
    except Exception:
        level, policy = None, None
    unit = build_evidence_unit(
        evidence_id=ev_id, data_type=data_type, text=text,
        source_id=skill, features=rec, geo_scope=geo,
        sensitivity_level=level, access_policy=policy,
    )
    # 协议要求每条 evidence 必须有数值 score；无语义分字段时按 rank 兜底(保留输入序)。
    score = _score_of(rec)
    if score is None:
        score = round(1.0 / (idx + 1), 6)
    return build_retrieval_evidence(
        evidence_ref=ev_id, payload=unit, rank=idx + 1, score=score
    )


def _summary_from(output_dir: Path, skill: str, n_evidence: int, n_total: int) -> dict:
    sj = output_dir / "summary.json"
    metrics: list[dict] = []
    overview = f"{skill} 产出 {n_total} 条记录，转为 {n_evidence} 条统一证据。"
    if sj.exists():
        try:
            data = json.loads(sj.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    metrics.append(build_key_metric(name=k, value=v))
            if data.get("overview"):
                overview = str(data["overview"])
    return build_summary(
        title=f"{skill} 统一检索结果",
        overview=overview,
        key_metrics=metrics[:20] or None,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="skill 原生输出 → 统一 SkillResult")
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--skill-name", required=True)
    ap.add_argument("--scenario", default="spatiotemporal_trajectory")
    ap.add_argument("--capability", default="trajectory_analysis")
    ap.add_argument("--data-type", default="spatiotemporal_trajectory")
    ap.add_argument("--evidence-file", default=None, help="主证据 jsonl 文件名；默认自动选记录最多的")
    ap.add_argument("--max-evidence", type=int, default=200)
    args = ap.parse_args()

    out = Path(args.output_dir).expanduser().resolve()
    if not out.is_dir():
        print(f"ERR: output-dir 不存在: {out}", file=sys.stderr)
        return 2

    ev_file = _pick_evidence_file(out, args.evidence_file)
    records = _read_jsonl(ev_file) if ev_file else []
    n_total = len(records)
    records = records[: args.max_evidence]

    # 构造 evidence 并保证 evidence_id 唯一(rule 3)：同 id 重复时按出现次序加 #n 后缀。
    evidence: list[dict] = []
    seen: dict[str, int] = {}
    for i, r in enumerate(records):
        ev = _to_evidence(r, args.skill_name, args.data_type, i)
        eid = ev["payload"]["evidence_id"]
        if eid in seen:
            seen[eid] += 1
            uniq = f"{eid}#{seen[eid]}"
            ev["payload"]["evidence_id"] = uniq
            ev["evidence_ref"] = uniq
        else:
            seen[eid] = 0
        evidence.append(ev)
    summary = _summary_from(out, args.skill_name, len(evidence), n_total)

    result = build_skill_result(
        skill_name=args.skill_name,
        scenario=args.scenario,
        capability=args.capability,
        summary=summary,
        evidence=evidence,
        status="success",
    )

    errors = validate_skill_result(result)
    dest = out / "skill_result.json"
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    src = ev_file.name if ev_file else "(无 jsonl 证据)"
    print(f"skill_result.json 已写出 ← 证据源 {src}（{len(evidence)}/{n_total} 条）")
    if errors:
        print("VALIDATE_FAIL:", "; ".join(errors)[:400])
        return 2
    print("VALIDATE_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
