from fastmcp import FastMCP

# 创建MCP服务器实例
mcp = FastMCP("MockMCPServer")


@mcp.tool()
def mock_fire_alarm(report: str) -> str:
    """将火警报告发送到消防部门"""
    return f"火警报告已发送到消防部门"

@mcp.tool()
def mock_flooding_alarm(report: str) -> str:
    """将洪涝报告发送到相关部门"""
    return f"洪涝报告已发送到相关部门"

@mcp.tool()
def mock_hotel_booking(report: str) -> str:
    """预定酒店"""
    return f"预定王府井附近的希尔顿酒店5天"

@mcp.tool()
def mock_ticket_booking(report: str) -> str:
    """预定机票"""
    return f"预定明天西安去北京的机票"

@mcp.tool()
def mock_street_order(report: str) -> str:
    """将街面秩序告警报告发送到城管部门"""
    return f"街面秩序告警报告已发送到城管部门"

@mcp.tool()
def mock_public_security_alarm(report: str) -> str:
    """将安全报告发送到公安部门"""
    return f"安全报告已发送到公安部门"

if __name__ == "__main__":
    # 使用sse传输方式启动服务器
    mcp.run(transport="sse", host="0.0.0.0", port=8000)