"""Current-turn execution and delivery requirements shared across stages."""

import re
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class TurnRequirements(BaseModel):
    requires_computation: bool = False
    allow_tools: bool = True
    required_csv_metrics: list[str] = Field(default_factory=list, max_length=16)
    csv_precision: dict[str, Annotated[int, Field(ge=0, le=12)]] = Field(default_factory=dict)
    csv_missing_values: list[str] = Field(default_factory=list, max_length=8)
    required_artifact_types: list[str] = Field(default_factory=list, max_length=8)

    @field_validator("csv_missing_values")
    @classmethod
    def nonnumeric_markers(cls, markers: list[str]) -> list[str]:
        for marker in markers:
            if not marker.strip() or re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?%?", marker.strip()):
                raise ValueError("Missing-value markers must be nonnumeric literals")
        return markers


REQUIREMENTS_EXAMPLE = {
    "requires_computation": False,
    "allow_tools": True,
    "required_csv_metrics": [],
    "csv_precision": {},
    "csv_missing_values": [],
    "required_artifact_types": [],
}
REQUIREMENTS_INSTRUCTION = (
    "本轮约束必须完整返回，不能机械复制上一轮。requires_computation 表示本轮是否要求实际计算/执行验证；"
    "只解释已有结果时为false。allow_tools 在用户明确禁止任何工具时为false；只禁止运行代码不等于禁止读取。"
    "不联网、不读取平台其他数据、不安装依赖、不改技能，都不等于禁止本地工具；读取本轮附件、计算、生成文件仍允许，allow_tools必须为true。"
    "requires_computation=true与allow_tools=false是不可执行的矛盾约束，不能同时返回；明确只解释且不调用工具时二者均为false。"
    "required_artifact_types 列出本轮明确要求导出/下载的文件类型（csv/json/py/pdf/docx/xlsx等，未指定类型用file），"
    "只要解释或明确取消导出时返回[]，不要继承旧交付要求。"
    "required_csv_metrics 是本轮CSV需覆盖的指标；csv_precision 将需小数格式的指标名逐项映射到位数，"
    "不含年份/ID/时间等原始列，支持不同指标不同精度；csv_missing_values 仅列用户明确约定的非数值标记，如NA。"
    "格式变更且要求保留原指标时返回更新后的完整清单；用户不要求CSV时这些CSV字段均清空。"
)


def explicit_decimal_places(query: str) -> int | None:
    """Parse literal formatting slots for older callers, never business scenes."""
    words = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10, "十一": 11, "十二": 12}
    token = r"(\d{1,2}|十二|十一|[零一二两三四五六七八九十])"
    patterns = [rf"(?:保留|精确到)\s*{token}\s*位小数", rf"小数点后\s*{token}\s*位", rf"小数(?:精度)?\s*(?:改成|改为|调整为|调整到|设置为|设为|保留)\s*{token}\s*位", r"(\d{1,2})\s*decimal places"]
    values = {words.get(value, int(value) if value.isdigit() else -1) for pattern in patterns for value in re.findall(pattern, query, re.IGNORECASE)}
    return next(iter(values)) if len(values) == 1 and 0 <= next(iter(values)) <= 12 else None


def requirements_from_context(context: dict | None) -> TurnRequirements:
    context = context or {}
    requirements = TurnRequirements.model_validate(context)
    query = str(context.get("original_query") or "")
    cancelled = explicit_export_cancellations(query)
    if '*' in cancelled:
        requirements.required_artifact_types = []
    else:
        requirements.required_artifact_types = [kind for kind in requirements.required_artifact_types if kind.lower().lstrip('.') not in cancelled]
    if '*' in cancelled or 'csv' in cancelled:
        requirements.required_csv_metrics = []
        requirements.csv_precision = {}
        requirements.csv_missing_values = []
        return requirements
    if not requirements.csv_precision and (digits := explicit_decimal_places(query)) is not None:
        requirements.csv_precision = {metric: digits for metric in requirements.required_csv_metrics}
    if not requirements.csv_missing_values:
        requirements.csv_missing_values = list(dict.fromkeys(re.findall(r'(?:写(?:为|成)?|记为|填(?:写|为)?|使用)\s*["“\']?(N/A|NA|null|不适用)(?![\w/])', query, re.IGNORECASE)))
    return requirements


def explicit_export_cancellations(query: str) -> set[str]:
    """Parse literal opt-outs, never scenes. Ambiguous exceptions stay semantic."""
    kinds = r'csv|json|pdf|docx|xlsx|py|文件'
    action = r'(?:生成|导出|输出|提供|创建|保存)'
    negative = re.compile(rf'(?:不需要|不再|不用|不要|无需|不必|别|不)(?:再|重新){{0,2}}{action}\s*(?:任何|新的?|可下载的)?\s*({kinds})(?:文件)?(?:了)?$', re.IGNORECASE)
    positive = re.compile(rf'{action}.{{0,24}}?({kinds}|下载)', re.IGNORECASE)
    cancelled, requested = set(), set()
    for clause in re.split(r'[，,。！？；;\n]', query):
        if match := negative.search(clause.strip()):
            kind = match.group(1).lower()
            cancelled.add('*' if kind == '文件' else kind)
            clause = clause.strip()[:match.start()]
        if match := positive.search(clause):
            requested.add(match.group(1).lower())
    # Do not resolve mixed global bans/exceptions or contradictory requests by
    # keyword precedence. Keep the model's complete contract in those cases.
    if ('*' in cancelled and requested) or requested & {'文件', '下载'}:
        return set()
    return cancelled - requested
