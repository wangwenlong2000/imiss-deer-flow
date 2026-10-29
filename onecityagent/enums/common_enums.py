from enum import IntEnum


class StatusEnum(IntEnum):
    """通用状态枚举"""
    DISABLED = 0  # 禁用
    ENABLED = 1   # 启用


class DeletedEnum(IntEnum):
    """删除状态枚举"""
    NOT_DELETED = 0  # 未删除
    DELETED = 1      # 已删除


class LLMStatusEnum(IntEnum):
    """LLM状态枚举"""
    DISABLED = 0  # 不启用
    ENABLED = 1   # 启用