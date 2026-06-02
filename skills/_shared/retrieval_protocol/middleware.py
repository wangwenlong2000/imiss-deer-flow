"""统一检索中间件 —— schema v1.1 新增。

DeerFlow / Planner 在调每个检索 skill 时,统一经过本中间件包一层,
一次性提供三件套(老师要求 5):

1. **结果缓存** —— ``(query, filters, top_k, mode) → SkillResult`` 的 LRU+TTL
   缓存,命中直接返回,服务首 token < 1s。
2. **监控日志** —— 每次调用追加一行 JSON 至日志文件,字段:``request_id`` /
   ``skill_name`` / ``query_hash`` / ``cache_hit`` / ``latency_ms`` /
   ``hit_count`` / ``status``,便于性能排查与验收数据汇报。
3. **审计留痕** —— 记录用户 / 场景 / 命中前 sensitivity 分布 / 过滤动作 /
   脱敏依据 → 写到合规组指定的审计 sink(本模块给一份纯 stdlib 默认实现,
   合规组的 sink 接 ``audit_sink`` 钩子即可)。

设计原则:
- **纯 stdlib**,不依赖 DeerFlow 主干,各 RAG 也能直接 import 复用;
- 缓存键由 Envelope 算出的 deterministic hash(忽略 ``request_id`` 等抖动字段);
- 失败不阻塞主路径 —— 日志/审计写入异常吞掉、不抛出。

线程安全:本实现内部用 ``threading.Lock`` 保护缓存表。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# 缓存键
# ---------------------------------------------------------------------------


def _stable_dump(obj: Any) -> str:
    """对字典/列表稳定序列化,用于 hash 计算。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)


def compute_cache_key(envelope: dict[str, Any]) -> str:
    """从 Envelope 推导稳定缓存键。

    刻意忽略 ``request_id`` / ``schema_version``,只对**语义**字段哈希:
    ``skill_name`` / ``scenario`` / ``capability`` / ``input.parameters`` /
    ``input.filters`` / ``budget``(若存在)。
    """
    payload_input = envelope.get("input", {}) or {}
    salient = {
        "skill_name": envelope.get("skill_name"),
        "scenario": envelope.get("scenario"),
        "capability": envelope.get("capability"),
        "parameters": payload_input.get("parameters", {}),
        "filters": payload_input.get("filters", {}),
        "budget": envelope.get("budget", {}),
    }
    return hashlib.sha1(_stable_dump(salient).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# LRU + TTL 缓存
# ---------------------------------------------------------------------------


class LRUCache:
    """有界 LRU 缓存 + TTL,线程安全。

    超过 ``max_entries`` 时丢最旧的;条目超过 ``ttl_seconds`` 视为过期。
    """

    def __init__(self, *, max_entries: int = 1024, ttl_seconds: float = 600.0) -> None:
        self._max = max_entries
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._store: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            written_at, value = entry
            if time.time() - written_at > self._ttl:
                self._store.pop(key, None)
                return None
            self._store.move_to_end(key)
            return value

    def put(self, key: str, value: dict[str, Any]) -> None:
        with self._lock:
            self._store[key] = (time.time(), value)
            self._store.move_to_end(key)
            while len(self._store) > self._max:
                self._store.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


# ---------------------------------------------------------------------------
# 监控日志
# ---------------------------------------------------------------------------


class JsonlSink:
    """一行 JSON 写入文件,线程安全(进程内)。

    日志文件不存在会自动创建。父目录必须先存在,否则 fallback 到禁写(不抛)。
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._disabled = False
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            self._disabled = True

    def emit(self, record: dict[str, Any]) -> None:
        if self._disabled:
            return
        line = json.dumps(record, ensure_ascii=False, default=str)
        try:
            with self._lock, self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            # 日志失败不阻塞主路径。
            pass


# ---------------------------------------------------------------------------
# 审计留痕
# ---------------------------------------------------------------------------


# 视为隐私敏感的 sensitivity_level 取值,对齐 schema.SENSITIVE_LEVELS。
_SENSITIVE_LEVELS = ("pii_masked", "restricted")


def summarize_sensitivity(evidence: Iterable[dict[str, Any]]) -> dict[str, int]:
    """统计一组 evidence 的 sensitivity 分布,作为审计字段。"""
    counts: dict[str, int] = {}
    for wrapper in evidence:
        payload = wrapper.get("payload") if isinstance(wrapper, dict) else None
        if not isinstance(payload, dict):
            continue
        level = payload.get("meta", {}).get("sensitivity_level") or "open"
        counts[level] = counts.get(level, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# 中间件入口
# ---------------------------------------------------------------------------


# 检索 skill 的实际执行函数签名:envelope -> SkillResult。
RetrieveFn = Callable[[dict[str, Any]], dict[str, Any]]
# 审计 sink 钩子(合规组接的就是它)。
AuditSink = Callable[[dict[str, Any]], None]


class RetrievalMiddleware:
    """统一检索中间件 —— 包一层,所有 retrieve 调用走它。

    用法::

        mw = RetrievalMiddleware(
            cache=LRUCache(max_entries=1024, ttl_seconds=600),
            log_sink=JsonlSink("logs/retrieval.jsonl"),
            audit_sink=my_compliance_sink,  # 合规组实现
        )
        result = mw.call(envelope, retrieve_fn=network_traffic_rag_search)
    """

    def __init__(
        self,
        *,
        cache: LRUCache | None = None,
        log_sink: JsonlSink | None = None,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self._cache = cache if cache is not None else LRUCache()
        self._log = log_sink
        self._audit = audit_sink

    def call(
        self,
        envelope: dict[str, Any],
        *,
        retrieve_fn: RetrieveFn,
        user_id: str | None = None,
        gate: str | None = None,
        scene: str | None = None,
        policy_version: str | None = None,
        detector_version: str | None = None,
        evidence_actions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """对 ``retrieve_fn(envelope)`` 包一层。

        ``user_id`` 仅用于审计,不参与缓存键计算。``gate`` / ``scene`` /
        ``policy_version`` / ``detector_version`` / ``evidence_actions`` 均为
        可选审计字段(spec 2026-05-27 §4),缺省不写入 audit record。
        """
        cache_key = compute_cache_key(envelope)
        started_at = time.time()
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._emit_log(envelope, cache_key, cached, started_at, cache_hit=True)
            self._emit_audit(
                envelope, cached, user_id=user_id, cache_hit=True,
                gate=gate, scene=scene,
                policy_version=policy_version, detector_version=detector_version,
                evidence_actions=evidence_actions,
            )
            return cached

        result = retrieve_fn(envelope)
        status = result.get("status") if isinstance(result, dict) else None
        if status in ("success", "partial"):
            self._cache.put(cache_key, result)
        self._emit_log(envelope, cache_key, result, started_at, cache_hit=False)
        self._emit_audit(
            envelope, result, user_id=user_id, cache_hit=False,
            gate=gate, scene=scene,
            policy_version=policy_version, detector_version=detector_version,
            evidence_actions=evidence_actions,
        )
        return result

    # ----- internals -----

    def _emit_log(
        self,
        envelope: dict[str, Any],
        cache_key: str,
        result: dict[str, Any],
        started_at: float,
        *,
        cache_hit: bool,
    ) -> None:
        if self._log is None:
            return
        latency_ms = int((time.time() - started_at) * 1000)
        evidence = (result.get("result", {}) or {}).get("evidence", []) if isinstance(result, dict) else []
        self._log.emit({
            "ts": int(time.time() * 1000),
            "request_id": envelope.get("request_id"),
            "skill_name": envelope.get("skill_name"),
            "scenario": envelope.get("scenario"),
            "query_hash": cache_key,
            "cache_hit": cache_hit,
            "latency_ms": latency_ms,
            "hit_count": len(evidence) if isinstance(evidence, list) else 0,
            "status": result.get("status") if isinstance(result, dict) else "error",
        })

    def _emit_audit(
        self,
        envelope: dict[str, Any],
        result: dict[str, Any],
        *,
        user_id: str | None,
        cache_hit: bool,
        gate: str | None = None,
        scene: str | None = None,
        policy_version: str | None = None,
        detector_version: str | None = None,
        evidence_actions: list[dict[str, Any]] | None = None,
    ) -> None:
        if self._audit is None:
            return
        evidence = (result.get("result", {}) or {}).get("evidence", []) if isinstance(result, dict) else []
        evidence_list = evidence if isinstance(evidence, list) else []
        sensitivity = summarize_sensitivity(evidence_list)
        sensitive_hits = sum(
            count for level, count in sensitivity.items() if level in _SENSITIVE_LEVELS
        )
        data_type = (
            ((envelope.get("input") or {}).get("parameters") or {}).get("data_type")
        )
        record: dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "request_id": envelope.get("request_id"),
            "user_id": user_id,
            "skill_name": envelope.get("skill_name"),
            "scenario": envelope.get("scenario"),
            "cache_hit": cache_hit,
            "sensitivity_distribution": sensitivity,
            "sensitive_hit_count": sensitive_hits,
            "filters": (envelope.get("input", {}) or {}).get("filters", {}),
            "evidence_actions": list(evidence_actions) if evidence_actions else [],
        }
        # spec 2026-05-27 §4 新增字段:缺省 None 不写入
        for key, value in (
            ("gate", gate),
            ("scene", scene),
            ("data_type", data_type),
            ("policy_version", policy_version),
            ("detector_version", detector_version),
        ):
            if value is not None:
                record[key] = value
        try:
            self._audit(record)
        except Exception:
            # 审计失败不阻塞主路径。
            pass
