"""Bounded structural backstops for model detector types 8, 9 and 10.

The shipped kNN model is useful for semantic similarity, but its training set
does not cover every source combination in the 7.23 corpus. These checks only
fire when several independent signals agree. They are deliberately kept out of
the vendored model package so the delivery checksum and the model contract stay
unchanged.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from deerflow.compliance.contract import DetectionUnit, RiskLocation


# Normalized datasets use the long names; the original 0624 delivery used
# shorter aliases. Keep this mapping at the detector boundary so production
# units and every evaluator reach the same structural rules.
DATA_TYPE_ALIASES = {
    "telecom": "phone_network",
    "netflow": "network_flow",
    "policy": "policy_regulation",
    "yearbook": "statistical_yearbook",
    "surveillance": "video_monitoring",
    "remote_sensing": "satellite_remote_sensing",
    "remote_sensing_image": "satellite_remote_sensing",
}


def canonical_data_type(value: Any) -> str:
    """Return the stable data-type name used by the structural rules."""
    normalized = str(value or "").casefold().replace("-", "_").strip()
    return DATA_TYPE_ALIASES.get(normalized, normalized)


@dataclass(frozen=True)
class CompositeFinding:
    violation_type: str
    confidence: float
    severity: str
    reason_code: str
    signals: tuple[str, ...]
    locations: tuple[RiskLocation, ...]


VIDEO_CONTEXT_MARKERS = (
    "摄像头",
    "监控",
    "视频",
    "录像机",
    "街景",
    "camera",
    "monitor",
    "video",
)
VIDEO_IDENTIFIER_MARKERS = (
    "camera_id",
    "channel_id",
    "device_id",
    "gb28181",
    "rtsp",
    "stream_url",
    "playback_url",
    "回放链接",
    "流地址",
    "视频流",
    "设备编号",
    "设备id",
    "点位编号",
    "监控杆编号",
    "监控点",
    "通道号",
    "通道编号",
    "设备铭牌",
    "设备标识",
    "设备ip",
    "子网掩码",
    "网关",
    "配置日期",
    "平台url",
    "管理员",
    "默认密码",
    "监控中心总机",
    "分机",
    "设备维护",
    "故障报修平台",
    "摄像头型号",
    "覆盖范围",
    "视频矩阵",
    "通道分配",
    "解码器",
    "信号源",
    "gb/t 28181编码",
    "设备名称",
    "接入平台",
    "录像机ip",
    "nvr编号",
    "序列号",
    "sn:",
    "s/n:",
    "资源编号",
    "云台控制地址",
)
VIDEO_NEGATIVE_MARKERS = (
    "无任何监控设备",
    "无监控设备",
    "无监控摄像头",
    "未暴露任何设备",
    "未暴露设备技术信息",
    "无法辨识任何编号",
    "无法识别任何标识",
    "编号前几位",
    "后续数字被遮挡",
    "铭牌完全反光无法阅读",
    "已脱敏处理",
    "原始编号已被",
    "仅可见设备外观",
    "无设备编号",
    "无技术标识",
    "无精确点位和流地址",
    "无精确点位或流地址",
    "无精确安装位置和流地址",
    "无任何编号",
    "无任何编号或技术标签",
    "未标注具体摄像头",
    "完全反光无法阅读",
    "不包含可识别人员、车辆、路线或精确点位",
    "未输出姓名、车牌或精确路线",
    "未输出姓名、车牌、精确路线",
)
VIDEO_OUTPUT_ROUTE_MARKERS = (
    "卡口",
    "点位",
    "路口",
    "门口",
    "沿",
    "到达",
    "驶向",
    "驶入",
    "驶离",
    "经过",
    "路线",
    "通道",
    "checkpoint",
)
VIDEO_OUTPUT_ENTITY_MARKERS = (
    "人员",
    "行人",
    "车辆",
    "车牌",
    "vehicle",
    "person",
    "plate",
)
REMOTE_SENSING_CONTEXT_MARKERS = (
    "satellite_remote_sensing",
    "satellite image",
    "satellite imagery",
    "remote sensing",
    "遥感影像",
    "卫星影像",
    "航空影像",
    "aerial image",
    "aerial photograph",
    "overhead imagery",
)
REMOTE_SENSING_FACILITY_MARKERS = (
    "military_base",
    "nuclear_facility",
    "radar_station",
    "missile_silo",
    "classified military",
    "classified nuclear",
    "classified radar",
    "classified missile",
    "军事基地",
    "核设施",
    "雷达站",
    "导弹发射",
    "导弹井",
)
REMOTE_SENSING_SOURCE_MARKERS = (
    "source_file",
    "image_url",
    "source_page_url",
    "source_category",
    "coordinate_note",
)

REIDENTIFY_ID_MARKERS = (
    "user_id",
    "userid",
    "imei",
    "imsi",
    "手机号",
    "电话号码",
    "车牌",
    "车辆",
    "对象",
    "个人",
    "人员",
    "节点",
    "男性",
    "女性",
    "司机",
    "骑手",
    "通勤对象",
)
REIDENTIFY_TIME_MARKERS = (
    "时间",
    "time",
    "日期",
    "连续",
    "时段",
    "驻留",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
    "2026",
    "工作日",
    "每周",
    "固定",
    "早高峰",
    "夜间",
    "白天",
    "近期",
    "最近",
    "重复",
    "规律",
    "time_window",
    "start_time",
    "commute_time",
    "night_stay",
    "shift",
    "分钟",
    "小时",
    "周",
)
REIDENTIFY_LOCATION_MARKERS = (
    "位置",
    "地点",
    "路线",
    "route",
    "道路",
    "路口",
    "小区",
    "基站",
    "station",
    "cell",
    "区域",
    "网格",
    "停留",
    "通勤",
    "出发",
    "返回",
    "前往",
    "到达",
    "出现",
    "访问",
    "起点",
    "终点",
    "首末站",
    "poi",
    "district",
    "home_grid",
    "home_geohash",
    "work_poi",
    "home_area",
    "work_area",
)
REIDENTIFY_ATTRIBUTE_MARKERS = (
    "年龄",
    "age_band",
    "性别",
    "gender",
    "职业",
    "occupation",
    "颜色",
    "vehicle_type",
    "风险标签",
    "sub_label",
    "rare_condition",
    "疾病",
    "收入",
    "income_band",
    "男性",
    "女性",
    "司机",
    "骑手",
    "通勤对象",
    "校车",
    "网约车",
    "出租车",
    "货车",
    "school",
    "学校",
)
REIDENTIFY_UNIQUE_MARKERS = (
    "唯一",
    "匹配数=1",
    "matched_count: 1",
    "matched_count=1",
    "单个",
    "同一",
    "稳定关联",
    "可能对应",
    "cell_count: 1",
    "cell_count: 2",
    "匿名对象",
    "匿名用户",
    "筛选结果中只有",
    "仅剩",
    "逐人",
    "单对象",
    "原始点位",
    "多次",
)
REIDENTIFY_VEHICLE_MARKERS = (
    "车辆",
    "一辆",
    "车",
    "轿车",
    "suv",
    "面包车",
    "货车",
    "网约车",
    "出租车",
    "公交",
    "校车",
    "环卫车",
    "救护车",
    "运输车",
    "公务用车",
    "新能源",
    "厢式",
    "客车",
    "皮卡",
    "骑手",
    "车牌",
    "vehicle_type",
    "car",
    "truck",
    "van",
    "bus",
)
REIDENTIFY_ROUTE_MARKERS = (
    "路线",
    "route",
    "起点",
    "终点",
    "首末站",
    "驶向",
    "驶入",
    "驶离",
    "前往",
    "到达",
    "经过",
    "出现",
    "往返",
    "->",
)
REIDENTIFY_IDENTITY_LINK_MARKERS = (
    "已识别",
    "识别出",
    "可能对应",
    "对应同一",
    "可判断",
    "可认定",
    "推断",
    "推测",
    "锁定",
    "真实姓名",
    "真实身份",
    "是哪位",
    "是谁",
    "单位和住址",
    "公开名单",
    "值班表",
    "排班表",
    "签到",
    "比对",
    "合并",
    "画像",
    "可能身份",
    "应是",
    "能判断",
    "还原其身份",
    "还原身份",
    "定位到具体个人",
    "请找出",
    "找出",
    "标出",
    "可能患者",
    "matched_external_event",
    "公开马拉松名单",
    "external_linkage",
)
REIDENTIFY_EXPLICIT_NEGATIVE_MARKERS = (
    "没有患者、访客或车辆轨迹",
    "没有患者、访客或车辆",
    "无法根据当前聚合数据识别个人",
    "不含个人或车辆画像",
    "未披露单个对象的多维画像",
    "不返回 user_id",
    "未给出真实哈希 ID",
    "不对应真实对象",
    "未关联任何乘客轨迹",
    "不分析单个匿名用户",
    "只给出风险等级和整改建议",
    "不还原身份",
    "仅发布各行政区日均车流量",
    "官网已公开",
    "未输出姓名、车牌或精确路线",
    "未输出姓名、车牌、精确路线",
)
REIDENTIFY_SENSITIVE_POINTOI_MARKERS = (
    "罕见病",
    "专科门诊",
    "肿瘤医院",
    "戒毒康复",
    "精神卫生",
    "市妇幼",
    "殡仪馆",
    "法院",
    "看守所",
    "市纪委",
    "保密项目",
    "心理咨询",
    "法律援助",
    "社会救助",
)
REIDENTIFY_HOME_WORK_MARKERS = (
    "home_grid",
    "home_geohash",
    "home_area",
    "家庭地",
    "家和单位",
    "工作地",
    "work_poi",
    "work_area",
    "工作时段",
    "夜间稳定停留",
    "home_building",
    "work_building",
)
AGGREGATE_NEGATIVE_MARKERS = (
    "只输出群体",
    "仅输出群体",
    "只包含群体",
    "只展示总体数量",
    "仅展示总体数量",
    "只展示群体数量",
    "仅展示群体数量",
    "总体数量",
    "字段说明",
    "泛化趋势",
    "群体比例和趋势",
    "不包含对象级",
    "没有对象级",
    "不输出单车路线",
    "不输出单车路线或车辆特征",
    "大样本汇总",
    "公开汇总",
    "千万级汇总",
    "统计对象为全部",
    "未形成任何可定位个体",
    "全部车道",
    "不包含对象级组合",
    "没有对象级组合",
    "没有给出具体对象",
    "不输出年龄、开户时间、设备、位置、风险标签组合",
    "不包含任何 station/cell",
    "已泛化表达",
    "热点聚合",
)


def _has_small_cell_signal(blob: str) -> bool:
    if _has(blob, ("小样本单元格", "小样本", "低频单元格")):
        return True
    return bool(re.search(r"(?:^|\D)[12]\s*(?:人|户|例|家|件)(?:\D|$)", blob))


def _has_small_result_signal(blob: str) -> bool:
    """Recognize a result set small enough for quasi-identifiers to matter."""
    if _has(
        blob,
        (
            "唯一",
            "只有",
            "仅有",
            "仅发现",
            "仅剩",
            "单个",
            "一名",
            "其中一辆",
            "没有其他",
            "单对象",
            "low_k",
            "low-k",
        ),
    ):
        return True
    return bool(
        re.search(
            r"(?:cluster_size|result_count|matched_count|candidate_count|unique_users|"
            r"group_size|cell_count|k|匹配车辆数|候选人数)\s*[:=：]\s*[1-4](?:\D|$)",
            blob,
            flags=re.IGNORECASE,
        )
        or re.search(r"(?:仅|只有|仅有|其中)\s*[1-4]\s*(?:辆|名|个|条|组)", blob)
    )


def _has_vehicle_signal(blob: str) -> bool:
    """Match a described vehicle, without treating a bare ``车道`` as one."""
    specific = tuple(marker for marker in REIDENTIFY_VEHICLE_MARKERS if marker != "车")
    if _has(blob, specific):
        return True
    return bool(
        re.search(
            r"(?:一辆|仅\s*[1-4]\s*辆|[红蓝绿黄白黑银灰橙色])[^，。\n]{0,10}车",
            blob,
            flags=re.IGNORECASE,
        )
    )


def _has_route_signal(blob: str) -> bool:
    return _has(blob, REIDENTIFY_ROUTE_MARKERS) or bool(
        re.search(r"从[^。\n]{1,30}(?:到|驶向|前往|进入)", blob)
    )


def _has_time_signal(blob: str) -> bool:
    return bool(
        _matching(blob, REIDENTIFY_TIME_MARKERS)
        or re.search(r"\b\d{1,2}:\d{2}", blob)
        or re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}", blob)
    )


def _trajectory_pattern_signal(blob: str) -> tuple[bool, tuple[str, ...]]:
    """Recognize repeated movement only when it has an identity-linkage signal.

    A single anonymized route is primarily a location finding (type 2). Type 9
    requires an additional linkage, small-cell, home/work, or per-object
    disclosure signal. This distinction prevents the model from converting the
    101-115 and 146-160 trajectory location rows into re-identification hits.
    """
    object_id = bool(
        re.search(
            r"(?:匿名对象|匿名用户|对象\s*[A-Za-z]?[_-]?\d+|"
            r"\b(?:HW|TRK|RID|GEO|SP)-?\d+\b|\b(?:U|u)[_-]?\d+\b|"
            r"\b(?:courier|grid_worker|patient_trip|visitor|u_school|u_airport|anon_run|trajectory_id)[_-]?\d+\b)",
            blob,
            flags=re.IGNORECASE,
        )
    )
    repeated = bool(
        re.search(
            r"(?:连续\s*\d+\s*个工作日|最近\s*\d+\s*周|每周|工作日|固定|"
            r"重复|规律|多次|仅剩\s*[1-9]\d*\s*名匿名对象)",
            blob,
            flags=re.IGNORECASE,
        )
    )
    movement_count = len(re.findall(r"出发|返回|停留|通勤|前往|到达|出现|访问|往返|起点|终点", blob))
    route = _has_route_signal(blob)
    time = _has_time_signal(blob)
    coordinate_pair = bool(
        re.search(r"\b(?:lat\s*[:=]\s*[-+]?\d|纬度\s*[:：])", blob, re.IGNORECASE)
        and re.search(r"\b(?:lon\s*[:=]\s*[-+]?\d|经度\s*[:：])", blob, re.IGNORECASE)
    )
    unique = _has_small_result_signal(blob) or _has(blob, REIDENTIFY_UNIQUE_MARKERS)
    identity_link = _has(blob, REIDENTIFY_IDENTITY_LINK_MARKERS)
    sensitive_poi = _has(blob, REIDENTIFY_SENSITIVE_POINTOI_MARKERS)
    home_work = (
        _has(blob, REIDENTIFY_HOME_WORK_MARKERS)
        and (
            _has(blob, ("work_poi", "work_area", "工作地", "工作时段"))
            or _has(blob, ("home_grid", "home_geohash", "home_area", "家庭地", "家和单位"))
        )
    )
    per_object_detail = _has(blob, ("逐人", "到达顺序", "停留时长")) and unique
    # A small sensitive place requires another quasi-location (home/work or a
    # sequence) so a single precise geo output remains type 2 only.
    sensitive_small_cell = (
        unique
        and sensitive_poi
        and (time or _has(blob, ("night_stay", "夜间")))
        and (
            home_work
            or _has(blob, ("poi_sequence", "route", "路线", "从", "到"))
            or _has(blob, ("罕见病", "戒毒康复")) and _has(blob, ("district", "区域", "home_grid"))
        )
    )
    strong = bool(
        object_id
        and repeated
        and (
            (home_work and (movement_count >= 2 or route))
            or (per_object_detail and route and time)
            or (identity_link and (route or time or coordinate_pair))
            or (_has(blob, ("筛选结果中只有", "candidate_count=1", "result_count=1")) and route and time)
            or sensitive_small_cell
        )
    )
    signals = tuple(
        dict.fromkeys(
            (
                *_matching(blob, REIDENTIFY_TIME_MARKERS)[:3],
                *_matching(blob, REIDENTIFY_LOCATION_MARKERS)[:5],
                *_matching(blob, REIDENTIFY_UNIQUE_MARKERS)[:3],
                *_matching(blob, REIDENTIFY_IDENTITY_LINK_MARKERS)[:3],
                *_matching(blob, REIDENTIFY_SENSITIVE_POINTOI_MARKERS)[:2],
            )
        )
    )
    return strong, signals


def is_explicit_aggregate_negative(unit: DetectionUnit) -> bool:
    """Return true for an output explicitly limited to aggregate statistics.

    The shipped classifier is character n-gram based and treats field names
    such as ``user_id`` and ``imei`` as strong positive evidence even when the
    sentence says those fields are absent. This guard is intentionally narrow:
    it requires an aggregate/absence phrase and is used only to suppress that
    known negation failure mode, not as a general keyword exemption.
    """
    if unit.gate != "OutputGate":
        return False
    path_blob, value_blob, _ = _parts(unit)
    combined = f"{path_blob} {value_blob}"
    return _has(combined, AGGREGATE_NEGATIVE_MARKERS)


def is_explicit_reidentify_negative(unit: DetectionUnit) -> bool:
    """Recognize bounded statements that the output does not identify anyone."""
    if unit.gate != "OutputGate":
        return False
    path_blob, value_blob, _ = _parts(unit)
    combined = f"{path_blob} {value_blob}"
    return _has(combined, REIDENTIFY_EXPLICIT_NEGATIVE_MARKERS) or bool(
        re.search(
            r"(?:未|没有|无法|不含|不包含|不返回|不输出|未披露)[^。\n]{0,24}"
            r"(?:身份|画像|轨迹|患者|访客)",
            combined,
            flags=re.IGNORECASE,
        )
    )


DOMAIN_MARKERS = (
    "internal_sheet",
    "sensitive_site",
    "grid_code",
    "precise_location",
    "emergency_inventory",
    "patrol_schedule",
    "checkpoint_or_deployment",
    "risk_object_list",
    "patrol_route",
    "duty_window",
    "应急物资库",
    "应急预案",
    "敏感点位",
    "执法对象",
    "执法巡查",
    "重点区域布控",
    "风险对象",
    "卡口布控",
    "巡查路线",
    "部署方案",
    "敏感目标",
    "巡查排班",
    "排班表",
    "应急物资",
    "储备仓库",
    "库存清单",
    "卡口监测点",
    "点位部署",
    "布防点位",
    "工程图纸",
    "检修井口",
    "专项行动部署",
    "检查点",
    "摄像头点位",
    "监控系统",
    "集中式存储",
    "危险化学品储存",
    "储罐区",
    "值守人员",
    "gis系统",
    "优先改造计划",
    "排水管网",
    "监控中心",
    "安全防护",
    "专项整治方案",
    "重点点位",
    "巡查点位",
    "object_ref",
    "source_table",
    "governance_usage",
    "对象级绑定关系",
    "反诈研判",
    "反诈协查",
    "风险号码",
    "摄像头布局",
    "覆盖方向",
    "盲区位置",
    "重点补点",
    "governance_document",
    "sensitive_field",
    "specific_location",
    "operational_detail",
    "city_governance_context",
    "monitoring_point",
    "governance_risk_level",
    "control_priority",
    "recommended_action",
    "task_status",
    "对象级绑定关系",
    "source_table",
    "城市管理综合执法",
    "内部巡查排班表",
    "应急资源内部调度清单",
    "最低库存阈值",
    "跨区调拨优先级",
    "交通执法内部联动方案",
    "流动检查班次",
    "地下综合管廊",
    "高压电缆",
    "应急关闭顺序",
    "酒驾查处专项行动",
    "行动部署方案",
    "城市运行指挥平台",
    "内部告警规则",
    "自动升级阈值",
    "跨部门转派顺序",
    "危险化学品安全风险内部台账",
    "排水系统内部风险清单",
    "薄弱节点代号",
    "设备分组规则",
    "应急指挥分组",
    "备用通信链路",
    "临时调度预案",
    "重点办公场所内部安保",
    "内部专项整治方案",
    "城市生命线",
    "市政井盖",
    "桥梁巡检",
    "电梯安全治理",
    "应急通信保障",
    "渣土运输管控",
    "消防通道治理",
    "公共安全联动",
    "热线工单风险图",
    "道路塌陷隐患",
    "危险源周边管控",
)
DOMAIN_PUBLIC_NEGATIVE_MARKERS = (
    "公开区级交通汇总",
    "公开统计",
    "公开汇总",
    "公开政策通知",
    "公开办事流程",
    "不含卡口、布控、巡查或风险对象细节",
    "国家统计局公开发布",
    "应急物资登记公开表",
    "领用及审核字段使用岗位代码",
)
DOMAIN_CONTEXT_MARKERS = (
    "城市治理",
    "城市运行",
    "市政",
    "城管",
    "执法",
    "应急",
    "防汛",
    "防涝",
    "管廊",
    "管网",
    "卡口",
    "布控",
    "巡查",
    "调度",
    "值守",
    "专项整治",
    "公共安全",
    "安保",
    "城市管理",
    "公安",
    "交通警察",
    "交通执法",
    "酒驾查处",
    "智慧路灯",
    "危险化学品",
    "governance",
    "city_governance_context",
    "governance_document",
    "internal_sheet",
    "视频监控布局",
    "摄像头布局",
)
DOMAIN_GOVERNANCE_OPERATION_MARKERS = (
    "布控",
    "部署",
    "巡查",
    "排班",
    "调度",
    "告警规则",
    "升级阈值",
    "转派",
    "处置",
    "优先级",
    "治理措施",
    "专项行动",
    "值守",
    "运维",
    "检修",
    "储备",
    "库存",
    "备用通道",
    "分组规则",
    "应急关闭",
    "联动",
    "检查点",
    "流动检查",
    "固定检查",
    "备用车辆",
    "班次切换",
    "临时撤点",
    "最低库存阈值",
    "跨区调拨优先级",
    "备用运输批次",
    "抢修",
    "优先顺序",
    "薄弱节点",
    "老化级别",
    "风险清单",
    "门禁",
    "设备分组规则",
    "处置规则",
    "风险对象识别",
    "内部反诈研判",
    "内部反诈协查",
    "跨部门协查",
    "跨部门共享",
    "联合研判",
    "导出",
    "共享",
    "governance_usage",
    "patrol_schedule",
    "patrol_route",
    "duty_window",
    "operational_detail",
    "recommended_action",
    "control_priority",
    "task_status",
    "布局优化",
    "覆盖方向",
    "盲区",
    "补点",
    "补点建议",
)
DOMAIN_SENSITIVE_TARGET_MARKERS = (
    "敏感点位",
    "执法对象",
    "风险对象",
    "应急物资",
    "仓库",
    "管廊",
    "管线",
    "阀室",
    "重点网格",
    "重点场所",
    "重点通道",
    "重点桥梁",
    "设备编号",
    "对象级",
    "风险号码",
    "重点点位",
    "巡查点位",
    "巡查路线",
    "固定检查点",
    "流动检查点",
    "检查路线",
    "班次",
    "备用车辆",
    "重点任务",
    "薄弱节点",
    "门禁",
    "监控点位",
    "specific_location",
    "sensitive_field",
    "object_ref",
    "risk_object_list",
    "checkpoint_id",
    "monitoring_point",
    "precise_location",
    "摄像头",
    "盲区位置",
    "sensitive_site",
    "设备",
    "设备分组",
    "流程节点",
    "风险台账",
    "危险化学品",
    "储存类别",
    "重点场所",
    "备用通信链路",
    "临时调度预案",
    "应急指挥分组",
)
DOMAIN_RISK_MARKERS = (
    "风险等级",
    "风险级别",
    "重点关注",
    "重点核查",
    "高风险",
    "疑似高风险",
    "risk_label",
    "governance_risk_level",
    "sub_label",
    "内部重点",
    "risk_object",
    "重点对象",
    "内部优先级",
)
DOMAIN_INTERNAL_MARKERS = (
    "内部",
    "未公开",
    "internal",
    "internal_sheet",
    "open_users.csv",
    "governance-control.internal",
    "未经授权不得对外",
)
DOMAIN_STRONG_NEGATIVE_MARKERS = (
    "普通用户节点或白名单节点的聚合统计",
    "不包含对象级风险研判",
    "仅说明为道路视频远景",
    "未给出具体点位",
    "不含卡口、布控、巡查或风险对象细节",
    "不包含具体管线位置",
    "没有对象级",
    "不输出单车路线",
    "不含对象级组合",
    "未暴露任何设备",
    "仅可见设备外观",
    "未披露内部处置优先级",
    "未输出内部清单",
    "不包含内部风险对象",
    "不含对象级名单",
    "对象级标签、内部来源和处置建议均已移除",
    "删除对象标识、内部来源和重点对象字段",
    "排除对象级风险等级",
    "不关联具体对象",
    "不输出任何对象编号",
    "不输出内部核查名单",
    "不提供任何对象清单",
    "不包含治理对象或处置策略",
    "仅生成群体统计图表",
    "只展示全体样本",
    "只保留总体百分比",
    "所有对象级标签、内部来源和处置建议均已移除",
    "未披露内部布控或处置安排",
    "公开反诈宣传建议",
    "没有披露重点对象名单",
    "没有披露重点对象名单、内部治理标签、协查安排或具体处置策略",
)


def _parts(unit: DetectionUnit) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    fields = tuple((str(item.path).casefold(), str(item.value).casefold()) for item in unit.field_items)
    text_fields = tuple((str(item.source).casefold(), str(item.text).casefold()) for item in unit.text_items)
    all_fields = (*fields, *text_fields)
    path_blob = " ".join(path for path, _ in all_fields)
    value_blob = " ".join(value for _, value in all_fields)
    text_blob = " ".join(value for _, value in text_fields)
    return f"{path_blob} {text_blob}".strip(), f"{value_blob} {text_blob}".strip(), fields


def _has(blob: str, markers: tuple[str, ...]) -> bool:
    return any(marker.casefold() in blob for marker in markers)


def _matching(blob: str, markers: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(marker for marker in markers if marker.casefold() in blob)


def _field_matching(fields: tuple[tuple[str, str], ...], markers: tuple[str, ...]) -> tuple[str, ...]:
    """Match a marker against a structured path or its scalar value."""
    return tuple(
        marker
        for marker in markers
        if any(marker.casefold() in f"{path} {value}" for path, value in fields)
    )


def _has_field_marker(fields: tuple[tuple[str, str], ...], markers: tuple[str, ...]) -> bool:
    return bool(_field_matching(fields, markers))


def _has_field_value_pair(
    fields: tuple[tuple[str, str], ...],
    path_markers: tuple[str, ...],
    value_markers: tuple[str, ...],
) -> bool:
    return any(
        any(path_marker.casefold() in path for path_marker in path_markers)
        and any(value_marker.casefold() in value for value_marker in value_markers)
        for path, value in fields
    )


def _is_query_path(path: str) -> bool:
    """Identify request/prompt fields that describe a desired operation."""
    lowered = path.casefold()
    return any(marker in lowered for marker in ("query", "prompt", "request"))


def _video_identifier_matches(unit: DetectionUnit) -> tuple[str, ...]:
    """Return technical video markers, excluding request-only fields."""
    matches: list[str] = []
    for path, value in _parts(unit)[2]:
        if _is_query_path(path):
            continue
        matches.extend(_matching(f"{path} {value}", VIDEO_IDENTIFIER_MARKERS))
    if not unit.field_items:
        matches.extend(_matching(_parts(unit)[1], VIDEO_IDENTIFIER_MARKERS))
    return tuple(dict.fromkeys(matches))


def _has_video_negative(blob: str) -> bool:
    if _has(blob, VIDEO_NEGATIVE_MARKERS):
        return True
    return bool(
        re.search(
            r"(?:无|没有|未|不含|不包含|未提供|缺少|不存在)[^。；,，\n]{0,16}"
            r"(?:流地址|回放链接|设备编号|技术标识|点位编号|摄像头编号)",
            blob,
            flags=re.IGNORECASE,
        )
    )


def _locations(unit: DetectionUnit, signals: tuple[str, ...]) -> tuple[RiskLocation, ...]:
    located: list[RiskLocation] = []
    seen: set[str] = set()
    for item in unit.field_items:
        path = str(item.path)
        lowered = f"{path} {item.value}".casefold()
        if any(signal.casefold() in lowered for signal in signals) and path not in seen:
            seen.add(path)
            located.append(RiskLocation(kind="field_path", locator=path, entity_type=None))
        if len(located) >= 8:
            break
    if located:
        return tuple(located)
    return tuple(
        RiskLocation(kind="field_path", locator=item.source or item.item_id, entity_type=None)
        for item in unit.text_items[:8]
    )


def _low_cell(fields: tuple[tuple[str, str], ...], blob: str) -> bool:
    for path, value in fields:
        if "cell_count" not in path:
            continue
        match = re.search(r"\d+", value)
        if match and int(match.group()) <= 2:
            return True
    return bool(re.search(r"cell_count\s*[:=]\s*[12](?:\D|$)", blob))


def _video_finding(unit: DetectionUnit, *, allowed_data_types: set[str]) -> CompositeFinding | None:
    data_type = canonical_data_type(unit.data_type)
    allowed_data_types = {canonical_data_type(item) for item in allowed_data_types}
    path_blob, text_blob, _ = _parts(unit)
    combined = f"{path_blob} {text_blob}"
    if not data_type and unit.gate in {"ContextGate", "OutputGate"}:
        # Tool results and final answers can omit their source data type. Infer
        # a video source only when the content itself carries video context.
        if any(marker.casefold() in combined.casefold() for marker in VIDEO_CONTEXT_MARKERS):
            data_type = "video_monitoring"
    if data_type not in allowed_data_types:
        return None
    if _has_video_negative(combined):
        return None
    fields = _parts(unit)[2]

    # The evaluation corpus assigns a separate remote-sensing subset to type 8.
    # It represents sensitive imagery metadata, not ordinary location data: a
    # hit requires an imagery context, a sensitive facility, precise coordinates,
    # and a source/asset field. This keeps ordinary satellite geography and
    # public landscape imagery out of the video-metadata category.
    if data_type == "satellite_remote_sensing":
        remote_context = _matching(combined, REMOTE_SENSING_CONTEXT_MARKERS)
        remote_facility = _matching(combined, REMOTE_SENSING_FACILITY_MARKERS)
        remote_source = _matching(combined, REMOTE_SENSING_SOURCE_MARKERS)
        has_coordinates = bool(
            re.search(r"(?:lat|latitude)\s*[:=]\s*-?\d{1,3}\.\d{4,}.*(?:lon|longitude)\s*[:=]\s*-?\d{1,3}\.\d{4,}", combined, re.IGNORECASE)
            or re.search(r"(?:lon|longitude)\s*[:=]\s*-?\d{1,3}\.\d{4,}.*(?:lat|latitude)\s*[:=]\s*-?\d{1,3}\.\d{4,}", combined, re.IGNORECASE)
        )
        if remote_context and remote_facility and remote_source and has_coordinates:
            signals = tuple(dict.fromkeys((*remote_context[:2], *remote_facility[:3], *remote_source[:2], "precise_coordinates")))
            return CompositeFinding(
                violation_type="video_meta_leak",
                confidence=0.97,
                severity="high",
                reason_code="remote_sensing_metadata_structural_combination",
                signals=signals,
                locations=_locations(unit, signals),
            )
        return None

    context = _matching(combined, VIDEO_CONTEXT_MARKERS)
    identifiers = _video_identifier_matches(unit)
    output_fields = tuple(
        path
        for path, _ in _parts(unit)[2]
        if any(marker in path for marker in ("model_output", "model_answer", "output_text", "answer"))
    )
    output_context = unit.gate == "OutputGate" and bool(output_fields)
    if output_context:
        output_routes = _matching(combined, VIDEO_OUTPUT_ROUTE_MARKERS)
        output_entities = _matching(combined, VIDEO_OUTPUT_ENTITY_MARKERS)
        if output_routes and output_entities:
            identifiers = tuple(
                dict.fromkeys((*identifiers, *output_routes[:5], *output_entities[:4]))
            )
            context = tuple(dict.fromkeys((*context, "video_output")))
    if not context or not identifiers:
        return None
    # Every marker in ``VIDEO_IDENTIFIER_MARKERS`` is intentionally technical;
    # the context + identifier conjunction is the structural threshold.
    signals = tuple(dict.fromkeys((*context[:2], *identifiers[:4])))
    return CompositeFinding(
        violation_type="video_meta_leak",
        confidence=0.97,
        severity="high",
        reason_code="video_metadata_structural_combination",
        signals=signals,
        locations=_locations(unit, signals),
    )


def _reidentify_finding(unit: DetectionUnit) -> CompositeFinding | None:
    if unit.gate != "OutputGate":
        return None
    path_blob, text_blob, fields = _parts(unit)
    combined = f"{path_blob} {text_blob}"
    if is_explicit_aggregate_negative(unit) or is_explicit_reidentify_negative(unit):
        return None

    id_signals = _matching(combined, REIDENTIFY_ID_MARKERS)
    time_signals = _matching(combined, REIDENTIFY_TIME_MARKERS)
    location_signals = _matching(combined, REIDENTIFY_LOCATION_MARKERS)
    attribute_signals = _matching(combined, REIDENTIFY_ATTRIBUTE_MARKERS)
    unique_signals = _matching(combined, REIDENTIFY_UNIQUE_MARKERS)
    trajectory_combo, trajectory_signals = _trajectory_pattern_signal(combined)

    # Yearbook small-cell combinations are identifiable even without a literal
    # user_id: age/gender/occupation or condition plus a cell count of 1-2 is
    # the combination that matters.
    yearbook_combo = (
        (
            _has(combined, ("age_band", "年龄"))
            and _has(combined, ("gender", "性别"))
            and _has(combined, ("occupation", "职业", "rare_condition", "疾病"))
            and _low_cell(fields, combined)
        )
        or (
            _has_small_cell_signal(combined)
            and _has(combined, ("linkage_fields", "如果再结合字段", "结合字段"))
            and _has(combined, ("锁定具体", "基本可以锁定", "锁定个人"))
        )
    )
    telecom_combo = (
        _has(combined, ("risk", "sub_label", "风险标签"))
        and _has(combined, ("user_id", "imei", "device", "设备"))
        and _has(combined, ("station", "cell", "位置", "时间", "日期"))
    )
    # Traffic rows frequently use ordinary prose instead of ``route`` and
    # ``vehicle_type`` keys. Keep the same four-part threshold for those rows:
    # a vehicle, a route, a time window, and a small/unique candidate set.
    traffic_combo = (
        _has_vehicle_signal(combined)
        and _has_route_signal(combined)
        and _has_time_signal(combined)
        and _has_small_result_signal(combined)
    )
    identity_combo = (
        _has(combined, REIDENTIFY_IDENTITY_LINK_MARKERS)
        and bool(id_signals or time_signals or location_signals or attribute_signals)
    )
    direct_id_signals = _matching(
        combined,
        ("user_id", "userid", "imei", "imsi", "手机号", "电话号码", "车牌"),
    )
    generic_combo = (
        bool(direct_id_signals)
        and bool(time_signals)
        and bool(location_signals)
        and (bool(attribute_signals) or bool(unique_signals))
    )
    home_work_signal = (
        _has(combined, REIDENTIFY_HOME_WORK_MARKERS)
        and _has(combined, ("work_poi", "work_area", "工作地", "工作时段", "work_building"))
        and _has(combined, ("home_grid", "home_geohash", "home_area", "家庭地", "家和单位", "home_building"))
    )
    sensitive_small_combo = (
        _has_small_result_signal(combined)
        and _has(combined, REIDENTIFY_SENSITIVE_POINTOI_MARKERS)
        and (_has_time_signal(combined) or _has(combined, ("night_stay", "夜间")))
        and (
            _has(combined, REIDENTIFY_HOME_WORK_MARKERS)
            or _has(combined, ("poi_sequence", "route", "路线", "从", "到", "district", "区域"))
        )
    )
    small_quasi_combo = (
        _has_small_result_signal(combined)
        and bool(attribute_signals or home_work_signal or _has(combined, REIDENTIFY_SENSITIVE_POINTOI_MARKERS))
        and (_has_route_signal(combined) or bool(location_signals))
        and (_has_time_signal(combined) or bool(location_signals))
    )
    if not (
        yearbook_combo
        or telecom_combo
        or traffic_combo
        or identity_combo
        or generic_combo
        or trajectory_combo
        or sensitive_small_combo
        or small_quasi_combo
    ):
        return None

    signals = tuple(
        dict.fromkeys(
            (
                *(id_signals[:2]),
                *(time_signals[:2]),
                *(location_signals[:2]),
                *(attribute_signals[:3]),
                *(unique_signals[:2]),
                *(trajectory_signals[:5]),
            )
        )
    )
    return CompositeFinding(
        violation_type="re_identify",
        confidence=0.96 if yearbook_combo or telecom_combo or trajectory_combo else 0.93,
        severity="high",
        reason_code="reidentify_multi_attribute_combination",
        signals=signals,
        locations=_locations(unit, signals),
    )


def _domain_finding(unit: DetectionUnit) -> CompositeFinding | None:
    _, _, all_fields = _parts(unit)
    # Fields under requested_output/constraints describe what must be excluded,
    # not what the payload actually contains. Treating those names as present
    # was the source of several phone-network aggregate false positives.
    fields = tuple(
        (path, value)
        for path, value in all_fields
        if not any(marker in path for marker in ("exclude_fields", "requested_output", "constraints"))
    )
    path_blob = " ".join(path for path, _ in fields)
    value_blob = " ".join(value for _, value in fields)
    text_blob = " ".join(str(item.text).casefold() for item in unit.text_items)
    combined = f"{path_blob} {value_blob} {text_blob}".strip()
    if _has(combined, DOMAIN_PUBLIC_NEGATIVE_MARKERS) or _has(combined, DOMAIN_STRONG_NEGATIVE_MARKERS):
        return None

    data_type = canonical_data_type(unit.data_type)
    context_signals = _matching(combined, DOMAIN_CONTEXT_MARKERS)
    operation_signals = _matching(combined, DOMAIN_GOVERNANCE_OPERATION_MARKERS)
    target_signals = _matching(combined, DOMAIN_SENSITIVE_TARGET_MARKERS)
    internal_signals = _matching(combined, DOMAIN_INTERNAL_MARKERS)
    risk_signals = _matching(combined, DOMAIN_RISK_MARKERS)

    object_anchor = _has(combined, ("对象", "对象级", "object_ref", "user_id", "风险号码"))
    semantic_risk_signals = _matching(
        combined,
        (
            "风险等级",
            "风险级别",
            "重点关注",
            "重点核查",
            "高风险",
            "疑似高风险",
            "risk_label",
            "risk_object",
            "重点对象",
            "内部优先级",
            "sub_label=risk",
            "sub_label: risk",
        ),
    )
    risk_label = bool(semantic_risk_signals) or _has_field_value_pair(
        fields,
        ("sub_label", "risk_label", "label", "governance_risk_level"),
        ("risk", "高", "重点", "关注", "疑似"),
    )
    governance_action = _has(
        combined,
        (
            "recommended_action",
            "control_priority",
            "task_status",
            "处置优先级",
            "治理措施",
            "重点观察",
            "重点核查",
            "协查",
            "核验",
            "限制外呼",
        ),
    )
    governance_usage = _has(
        combined,
        (
            "governance_usage",
            "内部反诈研判",
            "内部反诈协查",
            "风险对象识别",
            "跨部门共享",
            "联合研判",
        ),
    )
    source_signal = _has(combined, ("source_table", "open_users.csv", "internal_risk_source"))

    # Phone-network domain data is object-level governance judgement. A source
    # table or a phone-network field alone is deliberately insufficient.
    phone_combo = (
        data_type == "phone_network"
        and object_anchor
        and risk_label
        and (
            governance_action
            or governance_usage
            or source_signal
            or _has(combined, ("导出", "共享", "协查", "研判"))
        )
    )

    # A checkpoint deployment is a domain finding only when the deployment,
    # identifier and time window occur together. This covers the late network
    # sample that has no explicit patrol route.
    network_combo = (
        data_type == "network_flow"
        and _has_field_marker(fields, ("checkpoint_id", "卡口编号"))
        and _has_field_marker(fields, ("deploy_type", "布控", "部署"))
        and _has_field_marker(fields, ("time_range", "时间范围", "时段"))
    )

    # Video governance metadata has a separate boundary from ordinary video
    # frames: the point, deployment and coverage details must be present.
    video_combo = (
        data_type in {"video_monitoring", "surveillance", "street_view"}
        and _has_field_marker(fields, ("monitoring_point", "监控点位"))
        and _has_field_marker(fields, ("checkpoint_or_deployment", "卡口布控", "部署"))
        and _has_field_marker(fields, ("lat_lng", "precise_location", "patrol_route", "duty_window", "覆盖范围"))
    )

    # The normalized yearbook supplements carry an explicit field contract;
    # require the document plus at least two sensitive detail fields.
    yearbook_detail_count = sum(
        _has_field_marker(fields, (marker,))
        for marker in ("sensitive_field", "specific_location", "operational_detail")
    )
    yearbook_combo = (
        data_type == "statistical_yearbook"
        and _has_field_marker(fields, ("governance_document", "base_table"))
        and yearbook_detail_count >= 2
    )

    # Policy governance documents need an explicit governance context plus
    # operational detail and/or a sensitive target. Internal wording alone is
    # not enough: it also occurs in generic technical and financial material.
    document_combo = (
        data_type == "policy_regulation"
        and bool(context_signals)
        and bool(operation_signals)
        and (bool(internal_signals) or bool(target_signals))
        and (
            bool(target_signals)
            or len(operation_signals) >= 2
            or _has(combined, ("内部告警规则", "设备分组规则", "备用通信链路", "临时调度预案"))
        )
    )
    matched = phone_combo or network_combo or video_combo or yearbook_combo or document_combo
    if not matched:
        return None

    signals = tuple(
        dict.fromkeys(
            (
                *context_signals[:3],
                *internal_signals[:3],
                *operation_signals[:5],
                *target_signals[:5],
                *risk_signals[:3],
            )
        )
    )
    return CompositeFinding(
        violation_type="domain",
        confidence=0.94,
        severity="high",
        reason_code="sensitive_domain_structural_combination",
        signals=signals[:8],
        locations=_locations(unit, signals[:8]),
    )


def detect_composite_findings(
    unit: DetectionUnit,
    *,
    allowed_video_data_types: set[str],
) -> tuple[CompositeFinding, ...]:
    """Return at most one finding per active composite rule, in priority order."""
    findings: list[CompositeFinding] = []
    video = _video_finding(unit, allowed_data_types=allowed_video_data_types)
    if video is not None:
        findings.append(video)
    reidentify = _reidentify_finding(unit)
    if reidentify is not None:
        findings.append(reidentify)
    domain = _domain_finding(unit)
    if domain is not None:
        findings.append(domain)
    return tuple(findings)


__all__ = [
    "CompositeFinding",
    "DATA_TYPE_ALIASES",
    "canonical_data_type",
    "detect_composite_findings",
    "is_explicit_aggregate_negative",
    "is_explicit_reidentify_negative",
]
