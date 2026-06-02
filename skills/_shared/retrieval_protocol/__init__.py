"""retrieval_protocol — task.md《统一检索输入与结果证据格式》的参考实现。

该包把检索请求统一成 **Input Envelope**（task.md §3），把检索结果统一成
**SkillResult**（task.md §4，每条命中是 ``retrieval`` 型 evidence，载荷为
兼容 citybench 的 ``evidence_unit``），并提供：

- ``schema``    —— §2 data_type 登记表与 §4.4 features 注册建议；
- ``envelope``  —— §3 统一检索输入构造器；
- ``evidence``  —— §4 evidence_unit / evidence wrapper / SkillResult 构造器；
- ``validate``  —— §7 融合前十条校验规则；
- ``adapters``  —— §6 与 citybench / network-traffic / road-traffic / policy
  四个已落地实现的兼容映射。

本包仅依赖 Python 标准库，可直接拷贝进任意检索 skill 的运行环境。
"""

from .adapters import (
    adapt_citybench_hit,
    adapt_citybench_result,
    adapt_code_hit,
    adapt_code_result,
    adapt_network_traffic_hit,
    adapt_network_traffic_result,
    adapt_policy_hit,
    adapt_policy_result,
    adapt_remote_sensing_hit,
    adapt_remote_sensing_result,
    adapt_road_traffic_hit,
    adapt_road_traffic_result,
    adapt_streetview_hit,
    adapt_streetview_result,
    adapt_surveillance_hit,
    adapt_surveillance_result,
    adapt_telecom_hit,
    adapt_telecom_result,
    adapt_traffic_flow_hit,
    adapt_traffic_flow_result,
    build_network_traffic_skill_result,
)
from .envelope import (
    build_budget,
    build_data_source,
    build_filters,
    build_input_envelope,
    build_retrieval_params,
    build_time_range_absolute,
    build_time_range_relative,
    new_request_id,
)
from .errors import (
    ERROR_ACTIONS,
    ERROR_CODES,
    ERROR_STATUSES,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_SUCCESS,
    build_error,
    derive_status,
    validate_error,
    validate_errors_block,
)
from .sensitivity_rules import (
    DEFAULT_SENSITIVITY,
    K_THRESHOLD_AGGREGATED_SAFE,
    classify_sensitivity,
)
from .audit import (
    AUDIT_ACTIONS,
    AUDIT_ACTION_STATUSES,
    AUDIT_GATES,
    AUDIT_REASON_CODES,
    AUDIT_SCENES,
    build_evidence_action,
    validate_evidence_action,
)
from .middleware import (
    JsonlSink,
    LRUCache,
    RetrievalMiddleware,
    compute_cache_key,
    summarize_sensitivity,
)
from .evidence import (
    build_artifact,
    build_evidence_unit,
    build_geo_scope,
    build_key_metric,
    build_locator,
    build_retrieval_block,
    build_retrieval_evidence,
    build_skill_result,
    build_summary,
)
from .schema import (
    ACCESS_POLICIES,
    DATA_TYPE_GROUPS,
    DATA_TYPES,
    EVIDENCE_TYPE_RETRIEVAL,
    FEATURES_REGISTRY,
    RETRIEVAL_STRATEGIES,
    RUN_MODES,
    SCHEMA_VERSION,
    SENSITIVITY_LEVELS,
    TIME_RANGE_MODES,
    data_type_group,
    is_registered_data_type,
)
from .validate import (
    is_valid_evidence,
    validate_budget,
    validate_evidence,
    validate_evidence_list,
    validate_evidence_unit,
    validate_input_envelope,
    validate_jsonl_lines,
    validate_skill_result,
    validate_time_range,
)
from .planner import (
    Plan,
    RetrievalTask,
    build_plan,
    split_budget,
)
from .aggregator import (
    Aggregator,
    AggregatorHooks,
)

__all__ = [
    # schema —— task.md §2 / §4.4
    "ACCESS_POLICIES",
    "DATA_TYPE_GROUPS",
    "DATA_TYPES",
    "EVIDENCE_TYPE_RETRIEVAL",
    "FEATURES_REGISTRY",
    "RETRIEVAL_STRATEGIES",
    "RUN_MODES",
    "SCHEMA_VERSION",
    "SENSITIVITY_LEVELS",
    "TIME_RANGE_MODES",
    "data_type_group",
    "is_registered_data_type",
    # envelope —— task.md §3 + v1.1 budget
    "build_budget",
    "build_data_source",
    "build_filters",
    "build_input_envelope",
    "build_retrieval_params",
    "build_time_range_absolute",
    "build_time_range_relative",
    "new_request_id",
    # errors —— schema v1.1
    "ERROR_ACTIONS",
    "ERROR_CODES",
    "ERROR_STATUSES",
    "STATUS_ERROR",
    "STATUS_PARTIAL",
    "STATUS_SUCCESS",
    "build_error",
    "derive_status",
    "validate_error",
    "validate_errors_block",
    # planner / aggregator —— task.md 反馈 #2/#3 编排层(S2)
    "Plan",
    "RetrievalTask",
    "build_plan",
    "split_budget",
    "Aggregator",
    "AggregatorHooks",
    # sensitivity_rules —— spec 2026-05-27 §3
    "DEFAULT_SENSITIVITY",
    "K_THRESHOLD_AGGREGATED_SAFE",
    "classify_sensitivity",
    # audit —— spec 2026-05-27 §4
    "AUDIT_ACTIONS",
    "AUDIT_ACTION_STATUSES",
    "AUDIT_GATES",
    "AUDIT_REASON_CODES",
    "AUDIT_SCENES",
    "build_evidence_action",
    "validate_evidence_action",
    # middleware —— schema v1.1(三件套:缓存/日志/审计)
    "JsonlSink",
    "LRUCache",
    "RetrievalMiddleware",
    "compute_cache_key",
    "summarize_sensitivity",
    # evidence —— task.md §4
    "build_artifact",
    "build_evidence_unit",
    "build_geo_scope",
    "build_key_metric",
    "build_locator",
    "build_retrieval_block",
    "build_retrieval_evidence",
    "build_skill_result",
    "build_summary",
    # validate —— task.md §7 + v1.1
    "is_valid_evidence",
    "validate_budget",
    "validate_evidence",
    "validate_evidence_list",
    "validate_evidence_unit",
    "validate_input_envelope",
    "validate_jsonl_lines",
    "validate_skill_result",
    "validate_time_range",
    # adapters —— task.md §6 + v1.1 六类
    "adapt_citybench_hit",
    "adapt_citybench_result",
    "adapt_code_hit",
    "adapt_code_result",
    "adapt_network_traffic_hit",
    "adapt_network_traffic_result",
    "adapt_policy_hit",
    "adapt_policy_result",
    "adapt_remote_sensing_hit",
    "adapt_remote_sensing_result",
    "adapt_road_traffic_hit",
    "adapt_road_traffic_result",
    "adapt_streetview_hit",
    "adapt_streetview_result",
    "adapt_surveillance_hit",
    "adapt_surveillance_result",
    "adapt_telecom_hit",
    "adapt_telecom_result",
    "adapt_traffic_flow_hit",
    "adapt_traffic_flow_result",
    "build_network_traffic_skill_result",
]
