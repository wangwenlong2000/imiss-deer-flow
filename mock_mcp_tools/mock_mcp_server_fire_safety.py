from fastmcp import FastMCP
from onecityagent.config import config

# 创建MCP服务实例
mcp = FastMCP("消防安全相关事件")


@mcp.tool()
def fire_alarm(report: str) -> str:
    """
    消防安全相关消息处理，发送消防安全相关事件到消防部门，返回处理结果。
    :param report: 原始消防安全相关信息报告
    :return: 标准化告警信息字典
    """
    return f"消防报告消息已发送给消防部门处理"


if __name__ == "__main__":
    port = config.mock_mcp_config.fire_safety
    mcp.run(transport="sse", host="0.0.0.0", port=port)
