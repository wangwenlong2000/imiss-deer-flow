# python
from contextlib import AsyncExitStack
from typing import Dict, List, Optional
import asyncio

from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from mcp.types import TextContent

from onecityagent.logger import logger
from onecityagent.service.tool_service.base import BaseTool, ToolResult
from onecityagent.service.tool_service.tool_collection import ToolCollection


class MCPClientTool(BaseTool):
    """Represents a tool proxy that can be called on the MCP server from the client side."""

    session: Optional[ClientSession] = None
    server_id: str = ""  # Add server identifier
    original_name: str = ""

    async def execute(self, **kwargs) -> ToolResult:
        """Execute the tool by making a remote call to the MCP server."""
        if not self.session:
            return ToolResult(error="Not connected to MCP server")

        try:
            logger.info(f"Executing tool: {self.original_name}")
            result = await self.session.call_tool(self.original_name, kwargs)
            content_str = ", ".join(
                item.text for item in result.content if isinstance(item, TextContent)
            )
            return ToolResult(output=content_str or "No output returned.")
        except asyncio.CancelledError as e:
            logger.warning(f"Tool execution cancelled: {self.original_name} - {e}")
            return ToolResult(error="Tool execution cancelled.")
        except Exception as e:
            return ToolResult(error=f"Error executing tool: {str(e)}")


class MCPClients(ToolCollection):
    """
    A collection of tools that connects to multiple MCP servers and manages available tools through the Model Context Protocol.
    """

    sessions: Dict[str, ClientSession] = {}
    exit_stacks: Dict[str, AsyncExitStack] = {}
    description: str = "MCP client tools for server interaction"

    def __init__(self):
        super().__init__()  # Initialize with empty tools list
        # self.name = "mcp"  # Keep name for backward compatibility

    async def connect_sse(self, server_url: str, server_id: str = "", headers=None) -> None:
        """Connect to an MCP server using SSE transport."""
        if not server_url:
            raise ValueError("Server URL is required.")

        server_id = server_id or server_url

        # Always ensure clean disconnection before new connection
        if server_id in self.sessions:
            await self.disconnect(server_id)

        exit_stack = AsyncExitStack()
        self.exit_stacks[server_id] = exit_stack

        try:
            streams_context = sse_client(url=server_url, headers=headers)
            streams = await exit_stack.enter_async_context(streams_context)
            session = await exit_stack.enter_async_context(ClientSession(*streams))
            self.sessions[server_id] = session
            await self._initialize_and_list_tools(server_id)
        except Exception as e:
            # 清理已创建的资源
            if server_id in self.exit_stacks:
                try:
                    await self.exit_stacks[server_id].aclose()
                except Exception as close_error:
                    logger.warning(f"清理连接资源时出错: {close_error}")
                self.exit_stacks.pop(server_id, None)

            # 记录详细错误信息
            error_message = f"连接到SSE服务器 {server_id} 失败: {e}"
            logger.error(error_message)

            # 重新抛出异常，但提供更有用的错误信息
            raise ConnectionError(error_message) from e

    async def connect_stdio(
            self, command: str, args: List[str], server_id: str = ""
    ) -> None:
        """Connect to an MCP server using stdio transport."""
        if not command:
            raise ValueError("Server command is required.")

        server_id = server_id or command

        # Always ensure clean disconnection before new connection
        if server_id in self.sessions:
            await self.disconnect(server_id)

        exit_stack = AsyncExitStack()
        self.exit_stacks[server_id] = exit_stack

        server_params = StdioServerParameters(command=command, args=args)
        stdio_transport = await exit_stack.enter_async_context(
            stdio_client(server_params)
        )
        read, write = stdio_transport
        session = await exit_stack.enter_async_context(ClientSession(read, write))
        self.sessions[server_id] = session
        await self._initialize_and_list_tools(server_id)

    async def connect_http(
            self, base_url: str, server_id: str = "", headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Connect to an MCP server over Streamable HTTP transport.

        Parameters
        ----------
        base_url : str
            HTTP 入口地址，例如 https://mcp.example.com/api
        server_id : str, optional
            本地用于区分不同连接的标识，留空时默认使用 base_url
        headers : dict[str, str] | None
            可选的额外 HTTP 头（Authorization、User-Agent 等）
        """
        if not base_url:
            raise ValueError("base_url is required")

        server_id = server_id or base_url

        if server_id in self.sessions:
            await self.disconnect(server_id)

        exit_stack = AsyncExitStack()
        self.exit_stacks[server_id] = exit_stack

        try:
            streams_context = streamablehttp_client(
                url=base_url,
                headers=headers or {},
                timeout=30.0
            )
            read_stream, write_stream, _ = await exit_stack.enter_async_context(streams_context)
            session = await exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
            self.sessions[server_id] = session
            await self._initialize_and_list_tools(server_id)

        except Exception as e:
            # 5. 异常时清理已分配资源，与 connect_sse 保持一致
            if server_id in self.exit_stacks:
                try:
                    await self.exit_stacks[server_id].aclose()
                except Exception as close_err:
                    logger.warning(f"Cleanup error during http connect: {close_err}")
                self.exit_stacks.pop(server_id, None)

            error_msg = f"连接 HTTP 服务器 {server_id} 失败: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e

    async def _initialize_and_list_tools(self, server_id: str) -> None:
        """Initialize session and populate tool map."""
        session = self.sessions.get(server_id)
        if not session:
            raise RuntimeError(f"Session not initialized for server {server_id}")

        try:
            await session.initialize()
            response = await session.list_tools()
        except asyncio.CancelledError as e:
            # 专门处理取消，进行清理并抛出更明确的错误，避免未捕获的 CancelledError 导致主流程崩溃
            # 尝试清理资源
            if server_id in self.exit_stacks:
                try:
                    await self.exit_stacks[server_id].aclose()
                except Exception as close_err:
                    logger.warning(f"Error while cleaning up after cancellation for {server_id}: {close_err}")
                self.exit_stacks.pop(server_id, None)
            self.sessions.pop(server_id, None)
            # 抛出 ConnectionError 以便上层统一处理（上层也会清理）
            raise ConnectionError(f"Initialization cancelled for server {server_id}") from e
        except Exception as e:
            logger.error(f"Failed to initialize/list tools for {server_id}: {e}")
            # 清理资源，确保没有残留
            if server_id in self.exit_stacks:
                try:
                    await self.exit_stacks[server_id].aclose()
                except Exception as close_err:
                    logger.warning(f"Cleanup error during tool initialization for {server_id}: {close_err}")
                self.exit_stacks.pop(server_id, None)
            self.sessions.pop(server_id, None)
            raise ConnectionError(f"Failed to initialize/list tools for {server_id}: {e}") from e

        # Create proper tool objects for each server tool
        for tool in response.tools:
            original_name = tool.name
            # Always prefix with server_id to ensure uniqueness
            # tool_name = f"mcp_{server_id}_{original_name}"
            tool_name = original_name  # Use original name for clarity

            server_tool = MCPClientTool(
                name=tool_name,
                description=tool.description,
                parameters=tool.inputSchema,
                session=session,
                server_id=server_id,
                original_name=original_name,
            )
            self.tool_map[tool_name] = server_tool

        # Update tools tuple
        self.tools = tuple(self.tool_map.values())
        logger.info(
            f"Connected to server {server_id} with tools: {[tool.name for tool in response.tools]}"
        )

    async def disconnect(self, server_id: str = "") -> None:
        """Disconnect from a specific MCP server or all servers if no server_id provided."""
        if server_id:
            if server_id in self.sessions:
                try:
                    exit_stack = self.exit_stacks.get(server_id)

                    # Close the exit stack which will handle session cleanup
                    if exit_stack:
                        try:
                            await exit_stack.aclose()
                        except (RuntimeError, BaseExceptionGroup, Exception) as e:
                            # 处理各种异步关闭错误，包括cancel scope和generator错误
                            error_str = str(e).lower()
                            if ("cancel scope" in error_str or
                                    "taskgroup" in error_str or
                                    "generator didn't stop" in error_str or
                                    "asyncgen" in error_str or
                                    "async_generator" in error_str or
                                    "attempted to exit cancel scope" in error_str):
                                logger.info(
                                    f"Async cleanup error during disconnect from {server_id}, continuing with cleanup: {e}"
                                )
                            else:
                                logger.warning(
                                    f"Unexpected error during disconnect from {server_id}, continuing with cleanup: {e}")
                                # 不抛出异常，确保清理过程继续

                    # Clean up references
                    self.sessions.pop(server_id, None)
                    self.exit_stacks.pop(server_id, None)

                    # Remove tools associated with this server
                    self.tool_map = {
                        k: v
                        for k, v in self.tool_map.items()
                        if v.server_id != server_id
                    }
                    self.tools = tuple(self.tool_map.values())
                    logger.info(f"Disconnected from MCP server {server_id}")
                except Exception as e:
                    logger.error(f"Error disconnecting from server {server_id}: {e}")
        else:
            # Disconnect from all servers in a deterministic order
            for sid in sorted(list(self.sessions.keys())):
                await self.disconnect(sid)
            self.tool_map = {}
            self.tools = tuple()
            logger.info("Disconnected from all MCP servers")