from enum import IntEnum, Enum


class AgentType(IntEnum):
    """智能体类型枚举"""
    SYSTEM = 0  # 系统内置
    USER = 1    # 用户新增


class AgentRoleType(IntEnum):
    """智能体角色类型枚举"""
    INTENT_RECOGNITION = 0  # 意图识别智能体
    PLANNING = 1            # 规划智能体
    EXECUTION = 2           # 执行智能体
    FEEDBACK = 3            # 反馈智能体
    QA = 4                  # 问答智能体


class McpConnectionType(Enum):
    """MCP连接类型枚举"""
    SSE = "sse"      # Server-Sent Events连接
    STDIO = "stdio"  # 标准输入输出连接
    HTTP = "streamableHttp"
    @classmethod
    def get_valid_values(cls):
        """获取所有有效值"""
        return [item.value for item in cls]

    @classmethod
    def is_valid(cls, value):
        """检查值是否有效"""
        return value in cls.get_valid_values()