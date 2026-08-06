import re
from typing import Any, Dict, List, Tuple

from .base import BaseDetector


class ConfidentialDetector(BaseDetector):
    violation_type = "confidential"

    INTERNAL_IP = re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b")
    DB_ENDPOINT = re.compile(r"(?i)\b(mysql|postgresql|postgres|redis|mongodb|jdbc|oracle|sqlserver)[:/][^\s'\"]+")
    AMOUNT = re.compile(r"(?i)(contract[_-]?amount|budget|quote|price|amount|合同|预算|报价|金额|评分细则)")

    LAT_LON = re.compile(r"(?i)\b(?:lat|latitude|lon|lng|longitude)\s*[:=]\s*-?\d{1,3}\.\d{4,}")
    COORD_PAIR = re.compile(r"\[\s*-?\d{1,3}\.\d{4,}\s*,\s*-?\d{1,3}\.\d{4,}\s*\]|\b-?\d{1,3}\.\d{4,}\s*,\s*-?\d{1,3}\.\d{4,}\b")

    INTERNAL_DOMAIN_TOKENS = [
        ".local",
        ".internal",
        "intranet",
        "internal-gateway",
        "prod-internal",
        "corp",
    ]
    API_PATH_TOKENS = [
        "/admin/",
        "/internal/",
        "/private/",
        "/ops/",
        "/db/",
        "/sync/",
        "/finance/",
        "/manage/",
        "project_budget",
    ]
    BUSINESS_KEYS = [
        "vendor",
        "supplier",
        "contract",
        "contract_amount",
        "budget",
        "quote",
        "quotation",
        "score_rule",
        "project_budget",
        "db_endpoint",
        "database",
        "supplier_list",
        "vendor_list",
    ]
    BUSINESS_TEXT = [
        "project_budget",
        "db_endpoint",
    ]
    REMOTE_SENSING_MARKERS = [
        "remote_sensing",
        "satellite_remote_sensing",
        "remote sensing",
        "遥感",
        "卫星影像",
        "图层",
    ]
    REMOTE_SENSING_SENSITIVE_TARGETS = [
        "military_base",
        "nuclear_facility",
        "radar_station",
        "missile_silo",
        "军事基地",
        "核设施",
        "雷达站",
        "导弹井",
        "导弹发射场",
        "敏感设施",
        "重点设施",
        "保密观测点",
        "能源核心点",
        "应急关键节点",
        "地下管网关键节点",
        "应急物资仓库",
        "重点区域布控",
        "执法巡查路线",
        "视频覆盖范围",
        "交通卡口布控",
        "网格治理责任区",
        "城市运行风险点",
        "监控点位",
        "sensitive site",
        "internal city ops node",
        "emergency warehouse",
        "patrol-rs",
        "emg-wh",
        "dg-fict",
    ]
    REMOTE_SENSING_EXPOSURE_MARKERS = [
        "精确坐标",
        "完整坐标",
        "中心点",
        "点位清单",
        "精确节点",
        "路线/节点坐标",
        "可公开下载",
        "公开报告中保留",
        "lat:",
        "lon:",
        "lat=",
        "lon=",
        "pixel_bbox",
        "像素位置",
        "route_or_nodes",
        "6_decimal_places",
        "high_precision_sensitive_facility_coordinate",
        "exact point",
    ]
    REMOTE_SENSING_VIDEO_MARKERS = [
        "camera_id",
        "channel_id",
        "gb28181_id",
        "rtsp_url",
        "playback_url",
        "monitor_point",
        "rtsp://",
    ]
    REMOTE_SENSING_INTERNAL_MARKERS = [
        "internal_test_only",
        "风险等级=内部",
        "内部遥感治理图层",
        "internal city ops",
    ]
    REMOTE_SENSING_MODALITIES = [
        "satellite_remote_sensing_image",
        "satellite imagery",
        "satellite image",
        "卫星遥感影像",
        "卫星影像",
    ]
    REMOTE_CLASSIFICATION_MARKERS = [
        "国家秘密",
        "绝密",
        "机密",
        "秘密级",
        "top secret",
        "classified",
    ]
    REMOTE_ACCESS_RESTRICTIONS = [
        "仅限",
        "按需知悉",
        "授权人员",
        "涉密人员",
        "need-to-know",
        "classified access",
    ]
    REMOTE_REPRODUCTION_RESTRICTIONS = [
        "不得复制",
        "禁止复制",
        "严禁复印",
        "不得复印",
        "未经批准",
        "未经授权",
        "no reproduction",
        "do not copy",
    ]
    REMOTE_SECRECY_CONTROLS = [
        "保密期限",
        "解除保密",
        "解密",
        "secrecy period",
        "declassif",
    ]
    REMOTE_BUSINESS_SENSITIVE_MARKERS = [
        "内部敏感业务信息",
        "内部采购测算",
        "供应商比价",
        "算法阈值",
        "客户清单",
        "生产调度",
        "限制流转判读报告",
    ]

    def detect(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        signals: List[Tuple[str, Dict[str, Any]]] = []
        rules: List[str] = []
        if self._is_public_summary_record(sample):
            return self.no_hit(sample, "Record is a public/open budget summary, not confidential detail.")

        for field_path, value in self.iter_search_items(sample):
            lowered = value.lower()
            key_lowered = field_path.lower()
            if self._is_public_summary_context(field_path, value):
                continue

            for match in self.INTERNAL_IP.finditer(value):
                signals.append(
                    ("internal_ip", self.make_location(field_path, "internal_ip", match.group(0), "internal-ip-address", match.start(), match.end()))
                )
                rules.append("internal-ip-address")

            for match in self.DB_ENDPOINT.finditer(value):
                signals.append(
                    ("db_endpoint", self.make_location(field_path, "database_endpoint", match.group(0), "database-endpoint", match.start(), match.end()))
                )
                rules.append("database-endpoint")

            if any(token in lowered for token in self.INTERNAL_DOMAIN_TOKENS):
                signals.append(("internal_domain", self.make_location(field_path, "internal_domain", value, "internal-domain")))
                rules.append("internal-domain")

            if any(token in lowered for token in self.API_PATH_TOKENS):
                signals.append(("internal_api_path", self.make_location(field_path, "internal_api_path", value, "internal-api-path")))
                rules.append("internal-api-path")

            if any(token in key_lowered for token in self.BUSINESS_KEYS) or any(token in lowered for token in self.BUSINESS_TEXT):
                signals.append(("business_sensitive", self.make_location(field_path, "business_sensitive", value, "business-sensitive-field")))
                rules.append("business-sensitive-field")

            if self.AMOUNT.search(key_lowered) or (field_path != "content_text" and self.AMOUNT.search(value)):
                signals.append(("amount_or_budget", self.make_location(field_path, "amount_or_budget", value, "amount-budget-field")))
                rules.append("amount-budget-field")

            if self._is_remote_sensing_context(sample, field_path, value) and not self._is_remote_sensing_public_or_sanitized(value):
                remote_signals = self._remote_sensing_signals(field_path, value)
                signals.extend(remote_signals)
                rules.extend(location["matched_rule_id"] for _, location in remote_signals)

        signal_types = {signal_type for signal_type, _ in signals}
        high_risk = (
            {"internal_domain", "internal_api_path"} <= signal_types
            or {"internal_ip", "db_endpoint"} <= signal_types
            or {"business_sensitive", "amount_or_budget"} <= signal_types
            or {"db_endpoint", "business_sensitive"} <= signal_types
            or {"remote_sensitive_target", "remote_precise_exposure"} <= signal_types
            or {"remote_sensitive_target", "remote_imagery"} <= signal_types
            or {"remote_video_resource", "remote_precise_exposure"} <= signal_types
            or {"remote_internal_layer", "remote_precise_exposure"} <= signal_types
            or {"remote_classification", "remote_access_restriction"} <= signal_types
            or {"remote_classification", "remote_reproduction_restriction"} <= signal_types
            or {"remote_classification", "remote_secrecy_control"} <= signal_types
            or {"remote_classification", "remote_task_code"} <= signal_types
            or (
                {"remote_business_sensitive", "remote_business_type"} <= signal_types
                and bool(
                    {"remote_access_restriction", "remote_reproduction_restriction", "remote_internal_classification"}
                    & signal_types
                )
            )
        )

        if not high_risk:
            llm_result = self._maybe_review_with_llm(sample, [location for _, location in signals])
            if llm_result is not None and llm_result.get("confidence", 0.0) >= 0.75:
                if llm_result.get("is_hit"):
                    return self.make_result(
                        sample,
                        True,
                        min(0.95, max(0.75, llm_result.get("confidence", 0.0))),
                        self._locations_from_llm(llm_result),
                        rules + ["llm-confidential-review"],
                        f"LLM review: {llm_result.get('reason')}",
                        ["manual_review", "desensitize"],
                    )
                return self.make_result(
                    sample,
                    False,
                    1.0 - min(0.95, llm_result.get("confidence", 0.0)),
                    [],
                    rules + ["llm-confidential-review"],
                    f"LLM review rejected confidential: {llm_result.get('reason')}",
                    ["allow"],
                )
            return self.no_hit(sample, "No confidential information combination rule reached threshold.")

        locations = self._dedupe_locations([location for _, location in signals])
        confidence = 0.9
        if {"internal_ip", "db_endpoint", "business_sensitive"} <= signal_types:
            confidence = 0.94
        elif {"internal_domain", "internal_api_path", "business_sensitive"} <= signal_types:
            confidence = 0.93
        elif {"remote_sensitive_target", "remote_precise_exposure"} <= signal_types:
            confidence = 0.92
        elif {"remote_classification", "remote_access_restriction"} <= signal_types:
            confidence = 0.95
        elif {"remote_business_sensitive", "remote_business_type"} <= signal_types:
            confidence = 0.92
        elif {"remote_sensitive_target", "remote_imagery"} <= signal_types:
            confidence = 0.9
        elif {"remote_video_resource", "remote_precise_exposure"} <= signal_types:
            confidence = 0.91

        return self.make_result(
            sample,
            True,
            confidence,
            locations,
            rules,
            "Internal technical or business-sensitive fields formed a confidential-information combination.",
            ["manual_review", "desensitize"],
        )

    def _maybe_review_with_llm(self, sample: Dict[str, Any], signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.use_llm or self.llm_adapter is None or not self.llm_adapter.is_available():
            return None
        if not signals and sample.get("sample_kind") != "boundary":
            return None
        return self.llm_adapter.review_confidential(sample, signals)

    def _locations_from_llm(self, llm_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        locations = []
        for span in llm_result.get("risk_spans", []):
            if not isinstance(span, dict):
                continue
            text = str(span.get("text") or "").strip()
            if not text:
                continue
            locations.append(
                self.make_location(
                    "content_text",
                    str(llm_result.get("risk_type") or "llm_confidential"),
                    text[:160],
                    "llm-confidential-review",
                )
            )
        if not locations and llm_result.get("is_hit"):
            locations.append(
                self.make_location(
                    "content_text",
                    str(llm_result.get("risk_type") or "llm_confidential"),
                    str(llm_result.get("reason") or "LLM identified confidential risk")[:160],
                    "llm-confidential-review",
                )
            )
        return locations

    def _is_remote_sensing_context(self, sample: Dict[str, Any], field_path: str, value: str) -> bool:
        data_type = str(sample.get("data_type") or sample.get("original_data_type") or "").lower()
        if "remote_sensing" in data_type or "satellite_remote_sensing" in data_type:
            return True
        combined = f"{field_path}\n{value}".lower()
        return any(marker in combined for marker in self.REMOTE_SENSING_MARKERS)

    def _is_remote_sensing_public_or_sanitized(self, value: str) -> bool:
        lowered = (value or "").lower()
        sanitized_markers = [
            "不含",
            "已剔除",
            "未出现",
            "near_miss",
            "容易被关键词误报",
            "lat/lon=null",
            "sensitive_label=null",
            "坐标已泛化",
            "不输出具体经纬度",
            "公开数据集样例",
            "公开遥感背景",
            "公开训练瓦片",
            "city_or_tile",
            "区县级",
            "城市级",
        ]
        return any(marker in lowered for marker in sanitized_markers)

    def _remote_sensing_signals(self, field_path: str, value: str) -> List[Tuple[str, Dict[str, Any]]]:
        lowered = value.lower()
        key_lowered = field_path.lower()
        signals: List[Tuple[str, Dict[str, Any]]] = []

        def add_signal(signal_type: str, risk_type: str, rule_id: str) -> None:
            signals.append(
                (
                    signal_type,
                    self.make_location(field_path, risk_type, value[:240], rule_id),
                )
            )

        if any(token in lowered for token in self.REMOTE_SENSING_SENSITIVE_TARGETS):
            add_signal("remote_sensitive_target", "remote_sensitive_target", "remote-sensing-sensitive-target")
        if any(token in lowered for token in self.REMOTE_SENSING_MODALITIES):
            add_signal("remote_imagery", "remote_imagery", "remote-sensing-imagery-modality")

        classification_field = "classification_level" in key_lowered or key_lowered.endswith(".密级")
        if any(token in lowered for token in self.REMOTE_CLASSIFICATION_MARKERS):
            add_signal("remote_classification", "remote_classification", "remote-sensing-classification")
        elif classification_field and lowered.strip() == "内部":
            add_signal(
                "remote_internal_classification",
                "remote_internal_classification",
                "remote-sensing-internal-classification",
            )

        access_field = "access_scope" in key_lowered or key_lowered.endswith(".知悉范围")
        if (access_field or "知悉" in lowered) and any(token in lowered for token in self.REMOTE_ACCESS_RESTRICTIONS):
            add_signal(
                "remote_access_restriction",
                "remote_access_restriction",
                "remote-sensing-access-restriction",
            )

        reproduction_field = "reproduction_restriction" in key_lowered or key_lowered.endswith(".复制限制")
        if (reproduction_field or "复制" in lowered or "复印" in lowered) and any(
            token in lowered for token in self.REMOTE_REPRODUCTION_RESTRICTIONS
        ):
            add_signal(
                "remote_reproduction_restriction",
                "remote_reproduction_restriction",
                "remote-sensing-reproduction-restriction",
            )

        secrecy_field = any(
            token in key_lowered
            for token in ("secrecy_period", "declassification_condition", "保密期限", "解密条件")
        )
        if secrecy_field and any(token in lowered for token in self.REMOTE_SECRECY_CONTROLS):
            add_signal(
                "remote_secrecy_control",
                "remote_secrecy_control",
                "remote-sensing-secrecy-control",
            )

        task_field = any(
            token in key_lowered
            for token in ("task_code", "mission_code", "涉密任务代号", "国家秘密任务编号")
        )
        if task_field and lowered.strip() not in {"", "none", "null", "n/a"}:
            add_signal("remote_task_code", "remote_task_code", "remote-sensing-task-code")

        business_sensitive_field = "business_sensitivity" in key_lowered
        if business_sensitive_field or any(token in lowered for token in self.REMOTE_BUSINESS_SENSITIVE_MARKERS):
            add_signal(
                "remote_business_sensitive",
                "remote_business_sensitive",
                "remote-sensing-business-sensitive",
            )

        business_type_field = "internal_business_type" in key_lowered or "risk_family" in key_lowered
        if business_type_field and lowered.strip() not in {"", "none", "null", "n/a"}:
            add_signal(
                "remote_business_type",
                "remote_business_type",
                "remote-sensing-business-type",
            )

        if (
            any(token in lowered for token in self.REMOTE_SENSING_EXPOSURE_MARKERS)
            or self.LAT_LON.search(value)
            or self.COORD_PAIR.search(value)
        ):
            add_signal("remote_precise_exposure", "remote_precise_exposure", "remote-sensing-precise-exposure")
        if any(token in lowered for token in self.REMOTE_SENSING_VIDEO_MARKERS):
            add_signal("remote_video_resource", "remote_video_resource", "remote-sensing-video-resource")
        if any(token in lowered for token in self.REMOTE_SENSING_INTERNAL_MARKERS):
            add_signal("remote_internal_layer", "remote_internal_layer", "remote-sensing-internal-layer")
        return signals

    def _is_public_summary_context(self, field_path: str, value: str) -> bool:
        lowered = (value or "").lower()
        key_lowered = (field_path or "").lower()
        public_tokens = [
            "public_service",
            "open.city.example.test",
            "/public/",
            "public/budget/summary",
            "district_total",
            "公开汇总",
            "不含供应商",
            "不含",
        ]
        if any(token in lowered for token in public_tokens):
            return True
        if key_lowered.endswith(".note") and ("公开" in value or "不含" in value):
            return True
        return False

    def _is_public_summary_record(self, sample: Dict[str, Any]) -> bool:
        combined = "\n".join(value for _, value in self.iter_search_items(sample)).lower()
        public_markers = [
            "open.city.example.test",
            "/public/budget/summary",
            "public_service",
            "district_total",
            "公开汇总",
            "汇总金额已公开",
        ]
        return any(marker in combined for marker in public_markers)

    def _dedupe_locations(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []
        for location in locations:
            key = (location.get("field_path"), location.get("risk_type"), location.get("text"))
            if key in seen:
                continue
            seen.add(key)
            unique.append(location)
        return unique

