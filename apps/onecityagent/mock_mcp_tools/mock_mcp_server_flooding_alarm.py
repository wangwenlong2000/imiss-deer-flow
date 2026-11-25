from fastmcp import FastMCP
from onecityagent.config import config

# 创建MCP服务实例
mcp = FastMCP("洪涝灾害告警事件")



@mcp.tool()
def flooding_alarm(report: str) -> str:
    """
    处理洪涝灾害事件，可以将洪涝灾情研判简报、事件等信息通知到应急部门。
    :param report: 涝灾情研判简报、事件等信息
    :return: 标准化信息字典
    """
    return f"洪涝消息已发送给应急部门处理"

if __name__ == "__main__":
    port = config.mock_mcp_config.flooding_alarm
    mcp.run(transport="sse", host="0.0.0.0", port=port)
